package redisqueue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	exchangeARGVMarker     = "__srq_ex"
	errExchangeVersionLua  = "ERR_EXCHANGE_VERSION"
	exchangeVersionRetries = 3
)

// ErrNoRoute — ни одна очередь не matched (bindings пусты или pattern не совпал).
var ErrNoRoute = errors.New("exchange: no route")

// ErrExchangeVersionConflict — checkVersion: версия в Redis не совпала после ретраев.
var ErrExchangeVersionConflict = errors.New("exchange: version conflict")

// Exchange — маршрутизация Publish в несколько очередей по bindings.
// Тип (direct/fanout/topic) задаётся на каждом Publish; в Redis только bindings + ver.
type Exchange struct {
	rdb  *redis.Client
	name string

	mu       sync.Mutex
	bindings []Binding
	version  string // после Load/Persist; для WithCheckVersion

	compression      Compression
	compressMinBytes int
}

// NewExchange создаёт exchange. Конфиг в Redis не трогает — нужен явный Persist/Load.
func NewExchange(rdb *redis.Client, name string) *Exchange {
	return &Exchange{rdb: rdb, name: name}
}

func (e *Exchange) bindingsKey() string { return "srq:exchange:" + e.name + ":bindings" }
func (e *Exchange) versionKey() string  { return "srq:exchange:" + e.name + ":ver" }

// Name — имя exchange.
func (e *Exchange) Name() string { return e.name }

// Version — локально известная версия после Load/Persist.
func (e *Exchange) Version() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.version
}

// Bindings — снимок локальных bindings.
func (e *Exchange) Bindings() []Binding {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Binding, len(e.bindings))
	copy(out, e.bindings)
	return out
}

// Bind добавляет привязку в память (chainable). Redis — только через Persist.
func (e *Exchange) Bind(pattern, queue string) *Exchange {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.bindings = append(e.bindings, Binding{Pattern: pattern, Queue: queue})
	return e
}

// Unbind удаляет точное совпадение pattern+queue из памяти.
func (e *Exchange) Unbind(pattern, queue string) *Exchange {
	e.mu.Lock()
	defer e.mu.Unlock()
	dst := e.bindings[:0]
	for _, b := range e.bindings {
		if b.Pattern == pattern && b.Queue == queue {
			continue
		}
		dst = append(dst, b)
	}
	e.bindings = dst
	return e
}

// SetBindings заменяет локальный набор.
func (e *Exchange) SetBindings(bs []Binding) *Exchange {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.bindings = append([]Binding(nil), bs...)
	return e
}

// SetCompression — сжатие payload при Publish (как у Producer).
func (e *Exchange) SetCompression(c Compression) {
	e.compression = c
}

// SetCompressMinBytes — порог сжатия.
func (e *Exchange) SetCompressMinBytes(n int) {
	e.compressMinBytes = n
}

// Persist записывает локальные bindings в Redis и инкрементирует ver.
// Не вызывать из горячего продюсера — CI / init / admin.
func (e *Exchange) Persist(ctx context.Context) error {
	if e.rdb == nil || e.name == "" {
		return fmt.Errorf("exchange: redis and name required")
	}
	e.mu.Lock()
	bs := append([]Binding(nil), e.bindings...)
	e.mu.Unlock()

	pipe := e.rdb.TxPipeline()
	pipe.Del(ctx, e.bindingsKey())
	if len(bs) > 0 {
		fields := make([]interface{}, 0, len(bs)*2)
		for _, b := range bs {
			fields = append(fields, bindingField(b.Pattern, b.Queue), "1")
		}
		pipe.HSet(ctx, e.bindingsKey(), fields...)
	}
	incr := pipe.Incr(ctx, e.versionKey())
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("exchange persist %q: %w", e.name, err)
	}
	ver := fmt.Sprintf("%d", incr.Val())
	e.mu.Lock()
	e.version = ver
	e.mu.Unlock()
	return nil
}

// Load загружает bindings и ver из Redis в память.
func (e *Exchange) Load(ctx context.Context) error {
	if e.rdb == nil || e.name == "" {
		return fmt.Errorf("exchange: redis and name required")
	}
	ver, err := e.rdb.Get(ctx, e.versionKey()).Result()
	if err == redis.Nil {
		ver = "0"
	} else if err != nil {
		return fmt.Errorf("exchange load ver %q: %w", e.name, err)
	}
	raw, err := e.rdb.HGetAll(ctx, e.bindingsKey()).Result()
	if err != nil {
		return fmt.Errorf("exchange load bindings %q: %w", e.name, err)
	}
	bs := make([]Binding, 0, len(raw))
	for field := range raw {
		pat, q, ok := parseBindingField(field)
		if !ok || q == "" {
			continue
		}
		bs = append(bs, Binding{Pattern: pat, Queue: q})
	}
	e.mu.Lock()
	e.bindings = bs
	e.version = ver
	e.mu.Unlock()
	return nil
}

// Publish — atomic multi-dest по умолчанию (без checkVersion).
func (e *Exchange) Publish(ctx context.Context, kind ExchangeKind, routingKey string, tasks ...*Task) error {
	return e.PublishWith(ctx, kind, routingKey, nil, tasks...)
}

// PublishWith — Publish с опциями доставки / backpressure / compression.
func (e *Exchange) PublishWith(ctx context.Context, kind ExchangeKind, routingKey string, opts []ExchangePublishOption, tasks ...*Task) error {
	if len(tasks) == 0 {
		return nil
	}
	cfg := &exchangePublishConfig{
		atomic: true,
		publishConfig: publishConfig{
			compression:      e.compression,
			compressMinBytes: e.compressMinBytes,
		},
	}
	cfg.apply(opts)
	if err := cfg.validate(); err != nil {
		return err
	}

	if !cfg.atomic {
		return e.publishBestEffort(ctx, kind, routingKey, cfg, tasks)
	}
	return e.publishAtomic(ctx, kind, routingKey, cfg, tasks)
}

func (e *Exchange) snapshotBindings() ([]Binding, string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	bs := append([]Binding(nil), e.bindings...)
	return bs, e.version
}

func (e *Exchange) publishAtomic(ctx context.Context, kind ExchangeKind, routingKey string, cfg *exchangePublishConfig, tasks []*Task) error {
	var lastErr error
	for attempt := 0; attempt < exchangeVersionRetries; attempt++ {
		bs, ver := e.snapshotBindings()
		dests := matchBindings(kind, routingKey, bs)
		if len(dests) == 0 {
			return ErrNoRoute
		}
		wantVer := ""
		if cfg.checkVersion {
			wantVer = ver
		}
		err := e.evalExchange(ctx, wantVer, dests, cfg, tasks)
		if err == nil {
			return nil
		}
		if !cfg.checkVersion || !isExchangeVersionErr(err) {
			return err
		}
		lastErr = err
		if loadErr := e.Load(ctx); loadErr != nil {
			return loadErr
		}
	}
	if lastErr != nil {
		return fmt.Errorf("%w: %v", ErrExchangeVersionConflict, lastErr)
	}
	return ErrExchangeVersionConflict
}

func (e *Exchange) publishBestEffort(ctx context.Context, kind ExchangeKind, routingKey string, cfg *exchangePublishConfig, tasks []*Task) error {
	bs, _ := e.snapshotBindings()
	dests := matchBindings(kind, routingKey, bs)
	if len(dests) == 0 {
		return ErrNoRoute
	}
	pubOpts := cfg.asPublishOptions()
	for _, q := range dests {
		p := &Producer{
			redis:            e.rdb,
			queueName:        q,
			compression:      cfg.compression,
			compressMinBytes: cfg.compressMinBytes,
		}
		if err := p.publishLocal(ctx, pubOpts, tasks); err != nil {
			return err
		}
	}
	return nil
}

func (e *Exchange) evalExchange(ctx context.Context, wantVer string, dests []string, cfg *exchangePublishConfig, tasks []*Task) error {
	bp := cfg.bpArgs()
	args := make([]interface{}, 0, 5+len(dests)+len(bp)+len(tasks)*7)
	args = append(args, exchangeARGVMarker, e.name, wantVer, len(dests))
	for _, q := range dests {
		args = append(args, q)
	}
	args = append(args, len(tasks))
	args = append(args, bp...)

	for _, task := range tasks {
		if task.ID == "" {
			return fmt.Errorf("task ID is required")
		}
		scheduled := task.Scheduled.UnixMilli()
		if scheduled == 0 {
			scheduled = time.Now().UnixMilli()
		}
		tags, err := encodeTags(task.Tags)
		if err != nil {
			return err
		}
		groups, err := encodeGroups(task.Groups)
		if err != nil {
			return err
		}
		payload, err := encodePayloadForRedis(task.Payload, cfg.compression, cfg.compressMinBytes)
		if err != nil {
			return err
		}
		args = append(args, task.ID, task.Partition, task.PartitionPriority, task.QueuePriority, scheduled, payload, tags, groups)
	}

	result, err := getAddScript().Run(ctx, e.rdb, []string{}, args...).Result()
	if err != nil {
		if isExchangeVersionErr(err) {
			return err
		}
		return fmt.Errorf("failed to publish exchange %q: %w", e.name, err)
	}
	notAdded, ok := result.([]interface{})
	if !ok {
		return fmt.Errorf("exchange publish: unexpected result type")
	}
	if len(notAdded) == 0 {
		return nil
	}
	if s, _ := notAdded[0].(string); s == "!" {
		return parseBackpressureResult(notAdded)
	}
	// atomic exchange: NX skips per-queue не поднимаем как ErrTasksAlreadyExist
	return nil
}

func isExchangeVersionErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), errExchangeVersionLua)
}
