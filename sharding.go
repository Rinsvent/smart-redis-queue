package redisqueue

import (
	"fmt"
	"hash/fnv"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// ShardStrategy — как считать shard key для sticky-маршрутизации.
type ShardStrategy int

const (
	// ShardByGroup — Groups[0] (пусто → DefaultGroup). Рекомендуемый default.
	ShardByGroup ShardStrategy = iota
	// ShardByQueue — имя очереди целиком на одном шарде.
	ShardByQueue
	// ShardByManual — ключ задаёт вызывающий (WithShardKey / PublishManual).
	ShardByManual
	// ShardByPartition — Task.Partition. Ломает group-scoped фичи между шардами.
	ShardByPartition
)

func (s ShardStrategy) String() string {
	switch s {
	case ShardByGroup:
		return "group"
	case ShardByQueue:
		return "queue"
	case ShardByManual:
		return "manual"
	case ShardByPartition:
		return "partition"
	default:
		return fmt.Sprintf("ShardStrategy(%d)", int(s))
	}
}

// Shard — один standalone Redis (или logical DB) в списке шардов.
type Shard struct {
	Name     string
	Addr     string
	Password string
	DB       int
	Weight   int           // 0 → 1; учитывается в hash % totalWeight
	Client   *redis.Client // если задан — Addr/Password/DB не используются для подключения
	// ReadOnly — продюсеры не пишут; consumer’ы продолжают читать (drain перед удалением).
	ReadOnly bool
}

// ShardConfig — общий конфиг producer и consumer.
type ShardConfig struct {
	Strategy ShardStrategy
	Shards   []Shard
	// KeyMap — sticky: shard key → имя шарда (поверх hash).
	KeyMap map[string]string
	// StrictSingleGroup — при ByGroup ошибка, если у задачи больше одной group.
	StrictSingleGroup bool
}

// ShardRouter выбирает клиент Redis по shard key.
type ShardRouter struct {
	cfg      *ShardConfig
	shards   []Shard
	clients  []*redis.Client
	owned    []bool
	writeIdx []int // индексы шардов без ReadOnly
	mu       sync.RWMutex
	closed   bool
}

// NewShardRouter валидирует конфиг и открывает клиенты (если Client не передан).
func NewShardRouter(cfg *ShardConfig) (*ShardRouter, error) {
	if cfg == nil {
		return nil, fmt.Errorf("shard config is nil")
	}
	if len(cfg.Shards) == 0 {
		return nil, fmt.Errorf("shard config: at least one shard required")
	}

	shards := make([]Shard, len(cfg.Shards))
	copy(shards, cfg.Shards)

	names := make(map[string]struct{}, len(shards))
	clients := make([]*redis.Client, len(shards))
	owned := make([]bool, len(shards))
	writeIdx := make([]int, 0, len(shards))

	for i := range shards {
		s := &shards[i]
		if s.Name == "" {
			s.Name = fmt.Sprintf("shard-%d", i)
		}
		if _, ok := names[s.Name]; ok {
			return nil, fmt.Errorf("shard config: duplicate shard name %q", s.Name)
		}
		names[s.Name] = struct{}{}
		if s.Weight < 0 {
			return nil, fmt.Errorf("shard %q: weight must be >= 0", s.Name)
		}
		if s.Weight == 0 {
			s.Weight = 1
		}

		if s.Client != nil {
			clients[i] = s.Client
		} else {
			if s.Addr == "" {
				return nil, fmt.Errorf("shard %q: Addr or Client required", s.Name)
			}
			clients[i] = redis.NewClient(&redis.Options{
				Addr:     s.Addr,
				Password: s.Password,
				DB:       s.DB,
			})
			owned[i] = true
		}
		if !s.ReadOnly {
			writeIdx = append(writeIdx, i)
		}
	}

	for key, name := range cfg.KeyMap {
		if _, ok := names[name]; !ok {
			return nil, fmt.Errorf("shard config: KeyMap[%q] unknown shard %q", key, name)
		}
	}

	return &ShardRouter{
		cfg:      cfg,
		shards:   shards,
		clients:  clients,
		owned:    owned,
		writeIdx: writeIdx,
	}, nil
}

// Close закрывает только клиенты, созданные роутером.
func (r *ShardRouter) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var first error
	for i, c := range r.clients {
		if r.owned[i] && c != nil {
			if err := c.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

// Config возвращает исходный конфиг.
func (r *ShardRouter) Config() *ShardConfig {
	return r.cfg
}

// Shards — копия списка шардов (с нормализованными Name/Weight).
func (r *ShardRouter) Shards() []Shard {
	out := make([]Shard, len(r.shards))
	copy(out, r.shards)
	return out
}

// Client возвращает клиент по индексу шарда.
func (r *ShardRouter) Client(i int) *redis.Client {
	if i < 0 || i >= len(r.clients) {
		return nil
	}
	return r.clients[i]
}

// Clients — все клиенты (включая ReadOnly), в порядке конфига.
func (r *ShardRouter) Clients() []*redis.Client {
	out := make([]*redis.Client, len(r.clients))
	copy(out, r.clients)
	return out
}

// ShardKeyForTask считает ключ маршрутизации для стратегии.
// manualKey используется при ShardByManual (и как override, если не пустой).
func (r *ShardRouter) ShardKeyForTask(queueName string, task *Task, manualKey string) (string, error) {
	if manualKey != "" {
		return manualKey, nil
	}
	switch r.cfg.Strategy {
	case ShardByQueue:
		return queueName, nil
	case ShardByGroup:
		if task == nil {
			return DefaultGroup, nil
		}
		groups := normalizeShardGroups(task.Groups)
		if r.cfg.StrictSingleGroup && len(groups) > 1 {
			return "", fmt.Errorf("shard: StrictSingleGroup: task %q has %d groups", task.ID, len(groups))
		}
		return groups[0], nil
	case ShardByPartition:
		if task == nil || task.Partition == "" {
			return "_", nil
		}
		return task.Partition, nil
	case ShardByManual:
		return "", fmt.Errorf("shard: ShardByManual requires WithShardKey or PublishManual")
	default:
		return "", fmt.Errorf("shard: unknown strategy %v", r.cfg.Strategy)
	}
}

func normalizeShardGroups(groups []string) []string {
	out := make([]string, 0, len(groups))
	seen := make(map[string]struct{}, len(groups))
	for _, g := range groups {
		if g == "" {
			continue
		}
		if _, ok := seen[g]; ok {
			continue
		}
		seen[g] = struct{}{}
		out = append(out, g)
	}
	if len(out) == 0 {
		return []string{DefaultGroup}
	}
	return out
}

// ResolveWrite — шард для публикации (ReadOnly пропускаются).
func (r *ShardRouter) ResolveWrite(shardKey string) (*redis.Client, Shard, int, error) {
	return r.resolve(shardKey, true)
}

// ResolveAny — шард для чтения / отладки (включая ReadOnly).
func (r *ShardRouter) ResolveAny(shardKey string) (*redis.Client, Shard, int, error) {
	return r.resolve(shardKey, false)
}

// ResolveByName — шард по имени.
func (r *ShardRouter) ResolveByName(name string, writeOnly bool) (*redis.Client, Shard, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, Shard{}, -1, fmt.Errorf("shard router closed")
	}
	for i, s := range r.shards {
		if s.Name != name {
			continue
		}
		if writeOnly && s.ReadOnly {
			return nil, Shard{}, -1, fmt.Errorf("shard %q is read-only", name)
		}
		return r.clients[i], s, i, nil
	}
	return nil, Shard{}, -1, fmt.Errorf("unknown shard %q", name)
}

func (r *ShardRouter) resolve(shardKey string, writeOnly bool) (*redis.Client, Shard, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, Shard{}, -1, fmt.Errorf("shard router closed")
	}
	if shardKey == "" {
		return nil, Shard{}, -1, fmt.Errorf("shard key is empty")
	}

	if name, ok := r.cfg.KeyMap[shardKey]; ok {
		for i, s := range r.shards {
			if s.Name != name {
				continue
			}
			if writeOnly && s.ReadOnly {
				return nil, Shard{}, -1, fmt.Errorf("shard %q is read-only (KeyMap[%q])", name, shardKey)
			}
			return r.clients[i], s, i, nil
		}
		return nil, Shard{}, -1, fmt.Errorf("KeyMap[%q] → unknown shard %q", shardKey, name)
	}

	candidates := make([]int, 0, len(r.shards))
	if writeOnly {
		if len(r.writeIdx) == 0 {
			return nil, Shard{}, -1, fmt.Errorf("no writable shards")
		}
		candidates = append(candidates, r.writeIdx...)
	} else {
		for i := range r.shards {
			candidates = append(candidates, i)
		}
	}

	total := 0
	for _, i := range candidates {
		total += r.shards[i].Weight
	}
	if total <= 0 {
		return nil, Shard{}, -1, fmt.Errorf("no shards with positive weight")
	}

	h := fnv32(shardKey) % uint32(total)
	acc := uint32(0)
	for _, i := range candidates {
		acc += uint32(r.shards[i].Weight)
		if h < acc {
			return r.clients[i], r.shards[i], i, nil
		}
	}
	// fallback (не должно случиться)
	i := candidates[len(candidates)-1]
	return r.clients[i], r.shards[i], i, nil
}

func fnv32(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// DistributeWorkers делит n воркеров по шардам (минимум 1 на шард, если n >= len).
func DistributeWorkers(n, shardCount int) []int {
	if shardCount <= 0 {
		return nil
	}
	if n < 1 {
		n = 1
	}
	out := make([]int, shardCount)
	if n < shardCount {
		for i := 0; i < n; i++ {
			out[i] = 1
		}
		return out
	}
	base := n / shardCount
	rem := n % shardCount
	for i := 0; i < shardCount; i++ {
		out[i] = base
		if i < rem {
			out[i]++
		}
	}
	return out
}

// validateManualKey — запрет пустого и разделителя тегов.
func validateManualKey(key string) error {
	if key == "" {
		return fmt.Errorf("shard key is empty")
	}
	if strings.Contains(key, TagSeparator) {
		return fmt.Errorf("shard key contains forbidden separator %q", TagSeparator)
	}
	return nil
}
