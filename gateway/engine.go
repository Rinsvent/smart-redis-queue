package gateway

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
)

// Config — параметры Gateway.
type Config struct {
	Redis      *redis.Client
	ListenAddr string // host:port gRPC data-plane
	HTTPAddr   string // host:port HTTP (data-plane + ops); empty = без HTTP
	DumpPath   string // graceful dump path; empty = no dump
	// Shards — опционально: один gateway на все шарды (тот же ShardConfig, что у SDK).
	Shards *redisqueue.ShardConfig
	// StatsRetention — если >0, consumer’ы gateway пишут g/a/r в Redis (централизованно).
	StatsRetention time.Duration
	// SkipPersistCheck — только для тестов (miniredis / no AOF).
	SkipPersistCheck bool
	// LossWindowOverride — если SkipPersistCheck; иначе из Redis CONFIG.
	LossWindowOverride time.Duration
	Logger             *log.Logger
}

// Gateway — single-active data-plane перед Redis.
type Gateway struct {
	cfg     Config
	log     *log.Logger
	state   *stateMachine
	ops     *opLog
	loss    *lossWindow
	persist PersistInfo

	mu           sync.Mutex
	producers    map[string]*redisqueue.Producer
	consumers    map[string]*redisqueue.Consumer // key: queue\x00consumerID[\x00shardIdx]
	shardClients []*redis.Client                 // nil → только cfg.Redis
	shardRouter  *redisqueue.ShardRouter

	hbMu        sync.Mutex
	hbConsumers map[*redisqueue.Consumer]struct{}

	stopHealth    chan struct{}
	healthWG      sync.WaitGroup
	healthStarted bool
}

// New создаёт gateway (ещё не Serving). Boot() — persist check + dump load.
func New(cfg Config) (*Gateway, error) {
	if cfg.Redis == nil {
		return nil, fmt.Errorf("gateway: redis client required")
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stderr, "srq-gateway: ", log.LstdFlags|log.Lmsgprefix)
	}
	g := &Gateway{
		cfg:        cfg,
		log:        cfg.Logger,
		state:      newStateMachine(StateBoot),
		ops:        newOpLog(),
		loss:       newLossWindow(0),
		producers:  make(map[string]*redisqueue.Producer),
		consumers:  make(map[string]*redisqueue.Consumer),
		stopHealth: make(chan struct{}),
	}
	return g, nil
}

// Boot — проверка persist, загрузка dump, переход в Running или Recovering.
func (g *Gateway) Boot(ctx context.Context) error {
	if g.cfg.Shards != nil {
		router, err := redisqueue.NewShardRouter(g.cfg.Shards)
		if err != nil {
			g.state.set(StateFailed)
			return fmt.Errorf("gateway: shards: %w", err)
		}
		g.shardRouter = router
		g.shardClients = router.Clients()
		g.log.Printf("shards: %d", len(g.shardClients))
	}

	var info PersistInfo
	var err error
	if g.cfg.SkipPersistCheck {
		info = PersistInfo{
			AppendOnly:  true,
			AppendFsync: "everysec",
			LossWindow:  g.cfg.LossWindowOverride,
		}
		if info.LossWindow < 0 {
			info.LossWindow = time.Second
		}
	} else {
		clients := g.redisClients()
		for i, c := range clients {
			info, err = DetectLossWindow(ctx, c)
			if err != nil {
				g.state.set(StateFailed)
				return fmt.Errorf("gateway: persist check shard/client[%d]: %w", i, err)
			}
		}
	}
	g.persist = info
	g.loss.setWindow(info.LossWindow)
	g.log.Printf("persist ok: appendfsync=%s loss_window=%s", info.AppendFsync, info.LossWindow)

	if g.cfg.DumpPath != "" {
		if _, err := os.Stat(g.cfg.DumpPath); err == nil {
			d, err := readDump(g.cfg.DumpPath)
			if err != nil {
				g.state.set(StateFailed)
				return fmt.Errorf("gateway: load dump: %w", err)
			}
			g.ops.load(d.Ops, d.NextOffset)
			g.loss.load(d.Loss)
			if d.LossWindowNS > 0 {
				g.loss.setWindow(time.Duration(d.LossWindowNS))
			}
			g.log.Printf("loaded dump: ops=%d loss=%d → recovering", len(d.Ops), len(d.Loss))
			g.state.set(StateRecovering)
			if err := g.recover(ctx); err != nil {
				g.state.set(StateFailed)
				return err
			}
			_ = os.Remove(g.cfg.DumpPath)
			return nil
		}
	}

	g.state.set(StateRunning)
	g.ensureHealth()
	return nil
}

// State / PersistInfo — для health RPC.
func (g *Gateway) State() State         { return g.state.Get() }
func (g *Gateway) Persist() PersistInfo { return g.persist }

func (g *Gateway) ensureHealth() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.healthStarted {
		return
	}
	g.healthStarted = true
	g.healthWG.Add(1)
	go func() {
		defer g.healthWG.Done()
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		pingT := time.NewTicker(30 * time.Second)
		defer pingT.Stop()
		for {
			select {
			case <-g.stopHealth:
				return
			case now := <-t.C:
				g.loss.truncate(now)
				g.probeRedis(context.Background())
			case <-pingT.C:
				g.pingAll(context.Background())
			}
		}
	}()
}

func (g *Gateway) probeRedis(ctx context.Context) {
	err := g.cfg.Redis.Ping(ctx).Err()
	st := g.state.Get()
	if err != nil {
		if st == StateRunning {
			g.log.Printf("redis fault: %v → redis_down", err)
			g.state.set(StateRedisDown)
		}
		return
	}
	if st == StateRedisDown {
		g.log.Printf("redis ok → recovering")
		g.state.set(StateRecovering)
		if err := g.recover(ctx); err != nil {
			g.log.Printf("recover failed: %v → redis_down", err)
			g.state.set(StateRedisDown)
			return
		}
	}
}

func (g *Gateway) redisClients() []*redis.Client {
	if len(g.shardClients) > 0 {
		return g.shardClients
	}
	return []*redis.Client{g.cfg.Redis}
}

func (g *Gateway) producer(queue string) *redisqueue.Producer {
	g.mu.Lock()
	defer g.mu.Unlock()
	if p, ok := g.producers[queue]; ok {
		return p
	}
	p := redisqueue.NewProducer(g.cfg.Redis, queue)
	if g.cfg.Shards != nil {
		if err := p.SetShards(g.cfg.Shards); err != nil {
			g.log.Printf("SetShards %s: %v", queue, err)
		}
	}
	g.producers[queue] = p
	return p
}

func (g *Gateway) consumer(queue, consumerID string) *redisqueue.Consumer {
	return g.consumerOn(g.cfg.Redis, queue, consumerID, -1)
}

func (g *Gateway) consumerOn(rdb *redis.Client, queue, consumerID string, shardIdx int) *redisqueue.Consumer {
	key := queue + "\x00" + consumerID
	if shardIdx >= 0 {
		key += "\x00" + fmt.Sprintf("%d", shardIdx)
	}
	g.mu.Lock()
	if c, ok := g.consumers[key]; ok {
		g.mu.Unlock()
		return c
	}
	opts := []redisqueue.ConsumerOption{redisqueue.WithPing(false)}
	if g.cfg.StatsRetention > 0 {
		opts = append(opts, redisqueue.WithStatsRetention(g.cfg.StatsRetention))
	}
	c := redisqueue.NewConsumer(rdb, queue, consumerID, opts...)
	g.consumers[key] = c
	g.mu.Unlock()
	g.trackConsumer(c)
	return c
}

// Close — остановка health (без dump; для dump — Drain).
func (g *Gateway) Close() {
	select {
	case <-g.stopHealth:
	default:
		close(g.stopHealth)
	}
	g.healthWG.Wait()
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, c := range g.consumers {
		c.Close()
	}
	for _, p := range g.producers {
		_ = p.Close()
	}
	if g.shardRouter != nil {
		_ = g.shardRouter.Close()
		g.shardRouter = nil
	}
}

// Drain — graceful stop: закрыть Get/Publish, дождаться, dump, Close.
func (g *Gateway) Drain(ctx context.Context) error {
	g.state.set(StateDraining)
	// короткий wait на terminal ops in-flight
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			break
		default:
			time.Sleep(50 * time.Millisecond)
		}
		break
	}
	if g.cfg.DumpPath != "" {
		d := Dump{
			Ops:          g.ops.snapshot(),
			Loss:         g.loss.snapshot(),
			LossWindowNS: int64(g.persist.LossWindow),
			NextOffset:   0,
		}
		for _, op := range d.Ops {
			if op.Offset+1 > d.NextOffset {
				d.NextOffset = op.Offset + 1
			}
		}
		if err := writeDump(g.cfg.DumpPath, d); err != nil {
			return err
		}
		g.log.Printf("dumped ops=%d loss=%d → %s", len(d.Ops), len(d.Loss), g.cfg.DumpPath)
	}
	g.Close()
	return nil
}
