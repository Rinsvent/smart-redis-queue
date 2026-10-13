package desktopui

import (
	"context"
	"fmt"
	"sync"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
)

// PartitionView — партиция для UI.
type PartitionView struct {
	ID      string `json:"id"`
	Pending int64  `json:"pending"`
	Locked  bool   `json:"locked"`
	Blocked bool   `json:"blocked"`
}

// RateView — клиентская оценка скорости.
type RateView struct {
	ProcessedPerSec    float64 `json:"processedPerSec"`
	BacklogDeltaPerSec float64 `json:"backlogDeltaPerSec"`
}

// QueueSnapshot — Inspect + скорость для UI.
type QueueSnapshot struct {
	Name       string          `json:"name"`
	Pending    int64           `json:"pending"`
	InProgress int64           `json:"inProgress"`
	Consumers  int             `json:"consumers"`
	Partitions []PartitionView `json:"partitions"`
	Rate       RateView        `json:"rate"`
}

// TaskView — payload и мета по id.
type TaskView struct {
	TaskID    string `json:"taskId"`
	Payload   string `json:"payload"`
	Partition string `json:"partition"`
	Priority  string `json:"priority"`
	Group     string `json:"group"`
	Exists    bool   `json:"exists"`
}

// TagRemoveResult — агрегат RemoveByTag для UI.
type TagRemoveResult struct {
	Removed  int      `json:"removed"`
	Payloads []string `json:"payloads,omitempty"`
}

// App — биндинги Wails / логика desktop manager.
type App struct {
	ctx   context.Context
	store *Store

	mu     sync.Mutex
	rdb    *redis.Client
	admin  *redisqueue.Admin
	connID string

	// speed sampler state per queue
	prevMu sync.Mutex
	prev   map[string]sample
	prevAt time.Time
}

type sample struct {
	pending    int64
	inProgress int64
}

func NewApp(store *Store) *App {
	return &App{
		store: store,
		prev:  map[string]sample{},
	}
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
}

func (a *App) ctxOrBackground() context.Context {
	if a.ctx != nil {
		return a.ctx
	}
	return context.Background()
}

// --- Connections ---

func (a *App) ListConnections() []Connection {
	return a.store.List()
}

func (a *App) ActiveConnectionID() string {
	return a.store.ActiveID()
}

func (a *App) AddConnection(name, addr, password string, db int) (Connection, error) {
	if err := a.ping(addr, password, db); err != nil {
		return Connection{}, fmt.Errorf("connection failed: %w", err)
	}
	c, err := a.store.Add(name, addr, password, db)
	if err != nil {
		return Connection{}, err
	}
	if err := a.connect(c); err != nil {
		return Connection{}, err
	}
	return c, nil
}

func (a *App) RemoveConnection(id string) error {
	a.mu.Lock()
	if a.connID == id {
		a.closeRedis()
	}
	a.mu.Unlock()
	return a.store.Remove(id)
}

func (a *App) SelectConnection(id string) error {
	if err := a.store.Select(id); err != nil {
		return err
	}
	c, err := a.store.Active()
	if err != nil {
		return err
	}
	return a.connect(*c)
}

func (a *App) TestConnection(addr, password string, db int) error {
	return a.ping(addr, password, db)
}

func (a *App) ping(addr, password string, db int) error {
	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db})
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return rdb.Ping(ctx).Err()
}

func (a *App) connect(c Connection) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closeRedis()
	rdb := redis.NewClient(&redis.Options{
		Addr:     c.Addr,
		Password: c.Password,
		DB:       c.DB,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return err
	}
	a.rdb = rdb
	a.admin = redisqueue.NewAdmin(rdb)
	a.connID = c.ID
	a.prevMu.Lock()
	a.prev = map[string]sample{}
	a.prevAt = time.Time{}
	a.prevMu.Unlock()
	return nil
}

func (a *App) closeRedis() {
	if a.rdb != nil {
		_ = a.rdb.Close()
		a.rdb = nil
		a.admin = nil
		a.connID = ""
	}
}

func (a *App) ensureAdmin() (*redisqueue.Admin, error) {
	a.mu.Lock()
	if a.admin != nil {
		admin := a.admin
		a.mu.Unlock()
		return admin, nil
	}
	a.mu.Unlock()

	c, err := a.store.Active()
	if err != nil {
		return nil, err
	}
	if err := a.connect(*c); err != nil {
		return nil, err
	}
	a.mu.Lock()
	admin := a.admin
	a.mu.Unlock()
	return admin, nil
}

// EnsureActive подключает активный профиль (вызывать при старте UI).
func (a *App) EnsureActive() error {
	if a.store.ActiveID() == "" {
		return nil
	}
	c, err := a.store.Active()
	if err != nil {
		return err
	}
	return a.connect(*c)
}

// --- Queues ---

func (a *App) ListQueues() ([]QueueSnapshot, error) {
	admin, err := a.ensureAdmin()
	if err != nil {
		return nil, err
	}
	ctx := a.ctxOrBackground()
	stats, err := admin.Inspect(ctx, "", "")
	if err != nil {
		return nil, err
	}

	now := time.Now()
	a.prevMu.Lock()
	defer a.prevMu.Unlock()
	dt := now.Sub(a.prevAt).Seconds()
	out := make([]QueueSnapshot, 0, len(stats))
	next := map[string]sample{}

	for _, s := range stats {
		parts := make([]PartitionView, 0, len(s.Partitions))
		for _, p := range s.Partitions {
			parts = append(parts, PartitionView{
				ID:      p.Partition,
				Pending: p.Pending,
				Locked:  p.Locked,
				Blocked: p.Blocked,
			})
		}
		snap := QueueSnapshot{
			Name:       s.QueueName,
			Pending:    s.TotalPending,
			InProgress: s.TotalInProgress,
			Consumers:  len(s.Consumers),
			Partitions: parts,
		}
		cur := sample{pending: s.TotalPending, inProgress: s.TotalInProgress}
		next[s.QueueName] = cur
		if !a.prevAt.IsZero() && dt > 0 {
			if prev, ok := a.prev[s.QueueName]; ok {
				prevBacklog := prev.pending + prev.inProgress
				curBacklog := cur.pending + cur.inProgress
				snap.Rate.BacklogDeltaPerSec = float64(curBacklog-prevBacklog) / dt
				// оценка: уменьшение pending ≈ обработка
				processed := float64(prev.pending-cur.pending) / dt
				if processed < 0 {
					processed = 0
				}
				snap.Rate.ProcessedPerSec = processed
			}
		}
		out = append(out, snap)
	}
	a.prev = next
	a.prevAt = now
	return out, nil
}

func (a *App) Purge(queue, partition string) (int, error) {
	admin, err := a.ensureAdmin()
	if err != nil {
		return 0, err
	}
	return admin.Purge(a.ctxOrBackground(), queue, partition)
}

func (a *App) Retry(queue string) (int, error) {
	admin, err := a.ensureAdmin()
	if err != nil {
		return 0, err
	}
	return admin.Retry(a.ctxOrBackground(), queue)
}

func (a *App) GetTaskPayload(queue, taskID string) (TaskView, error) {
	a.mu.Lock()
	rdb := a.rdb
	a.mu.Unlock()
	if rdb == nil {
		if _, err := a.ensureAdmin(); err != nil {
			return TaskView{}, err
		}
		a.mu.Lock()
		rdb = a.rdb
		a.mu.Unlock()
	}
	ctx := a.ctxOrBackground()
	payload, err := rdb.Get(ctx, "queue:"+queue+":payload:"+taskID).Result()
	if err == redis.Nil {
		return TaskView{TaskID: taskID, Exists: false}, nil
	}
	if err != nil {
		return TaskView{}, err
	}
	part, _ := rdb.Get(ctx, "queue:"+queue+":partition:"+taskID).Result()
	prio, _ := rdb.Get(ctx, "queue:"+queue+":priority:"+taskID).Result()
	return TaskView{
		TaskID:    taskID,
		Payload:   payload,
		Partition: part,
		Priority:  prio,
		Exists:    true,
	}, nil
}

func (a *App) CountByTag(queue, tag string) (int64, error) {
	admin, err := a.ensureAdmin()
	if err != nil {
		return 0, err
	}
	return admin.CountByTag(a.ctxOrBackground(), queue, tag)
}

func (a *App) RemoveByTag(queue, tag string, limit int, returnPayload bool) (TagRemoveResult, error) {
	admin, err := a.ensureAdmin()
	if err != nil {
		return TagRemoveResult{}, err
	}
	res, err := admin.RemoveByTag(a.ctxOrBackground(), queue, tag, redisqueue.RemoveByTagOptions{
		Limit:         limit,
		ReturnPayload: returnPayload,
	})
	if err != nil {
		return TagRemoveResult{}, err
	}
	out := TagRemoveResult{}
	for _, r := range res {
		if r.Status == redisqueue.RemovalStatusRemoved {
			out.Removed++
			if returnPayload && len(r.Payload) > 0 {
				out.Payloads = append(out.Payloads, string(r.Payload))
			}
		}
	}
	return out, nil
}
