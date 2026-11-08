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
	HTTPAddr   string // host:port HTTP ops; empty = без HTTP
	DumpPath   string // graceful dump path; empty = no dump
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

	mu        sync.Mutex
	producers map[string]*redisqueue.Producer
	consumers map[string]*redisqueue.Consumer // key: queue\x00consumerID

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
		info, err = DetectLossWindow(ctx, g.cfg.Redis)
		if err != nil {
			g.state.set(StateFailed)
			return err
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
		for {
			select {
			case <-g.stopHealth:
				return
			case now := <-t.C:
				g.loss.truncate(now)
				g.probeRedis(context.Background())
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

func (g *Gateway) producer(queue string) *redisqueue.Producer {
	g.mu.Lock()
	defer g.mu.Unlock()
	if p, ok := g.producers[queue]; ok {
		return p
	}
	p := redisqueue.NewProducer(g.cfg.Redis, queue)
	g.producers[queue] = p
	return p
}

func (g *Gateway) consumer(queue, consumerID string) *redisqueue.Consumer {
	key := queue + "\x00" + consumerID
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.consumers[key]; ok {
		return c
	}
	// startPing=false: heartbeat централизуем позже; пока без лишнего ping на каждый SDK consumer.
	c := redisqueue.NewConsumer(g.cfg.Redis, queue, consumerID, redisqueue.WithPing(false))
	g.consumers[key] = c
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
