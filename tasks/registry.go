package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
)

func isAlreadyExists(err error) bool {
	var e *redisqueue.ErrTasksAlreadyExist
	return errors.As(err, &e)
}

// ArgDef — схема аргумента для UI / defaults scheduler.
type ArgDef struct {
	Name    string `json:"name"`
	Type    string `json:"type,omitempty"` // string|int|bool|json
	Default string `json:"default,omitempty"`
	Help    string `json:"help,omitempty"`
}

// CatalogEntry — JSON в srq:tasks:{ns}:catalog.
type CatalogEntry struct {
	Name        string   `json:"name"`
	Queue       string   `json:"queue"`
	Cron        string   `json:"cron,omitempty"` // standard 5-field cron
	Concurrency string   `json:"concurrency,omitempty"`
	Args        []ArgDef `json:"args,omitempty"`
}

// Def — регистрация обработчика (+ опциональный cron-schedule).
// Разные опции / политики = разные Name (хэндлер ищется по base name).
type Def struct {
	Name        string
	Queue       string // "" → Config.DefaultQueue
	Cron        string // "*/5 * * * *"; пусто = только вручную/UI
	Concurrency string // forbid|after|allow
	Args        []ArgDef
	Handle      Handler
}

// Invocation — вызов handler из Dispatch.
type Invocation struct {
	Name     string
	Task     *redisqueue.Task
	Args     map[string]string
	RunID    string // Task.ID (может быть Name~suffix)
	BaseName string
}

// Handler — тело задачи.
type Handler func(ctx context.Context, inv Invocation) error

// Config — Registry.
type Config struct {
	Namespace    string
	DefaultQueue string
}

// Registry — каталог + dispatch + scheduler + last-run.
type Registry struct {
	cfg   Config
	rdb   *redis.Client
	mu    sync.RWMutex
	defs  map[string]*Def
	prods map[string]*redisqueue.Producer // queue → producer

	schedStop context.CancelFunc
	schedWG   sync.WaitGroup
}

// New создаёт Registry (rdb для catalog/run; producers регистрируются отдельно или через EnsureProducer).
func New(rdb *redis.Client, cfg Config) (*Registry, error) {
	if rdb == nil {
		return nil, fmt.Errorf("tasks: redis is nil")
	}
	cfg.Namespace = strings.TrimSpace(cfg.Namespace)
	if cfg.Namespace == "" {
		return nil, fmt.Errorf("tasks: namespace required")
	}
	return &Registry{
		cfg:   cfg,
		rdb:   rdb,
		defs:  make(map[string]*Def),
		prods: make(map[string]*redisqueue.Producer),
	}, nil
}

// Register добавляет Def (имя уникально).
func (r *Registry) Register(d Def) error {
	if r == nil {
		return fmt.Errorf("tasks: nil registry")
	}
	d.Name = strings.TrimSpace(d.Name)
	if d.Name == "" {
		return fmt.Errorf("tasks: empty name")
	}
	if d.Handle == nil {
		return fmt.Errorf("tasks: %q nil handler", d.Name)
	}
	if d.Queue == "" {
		d.Queue = r.cfg.DefaultQueue
	}
	if d.Queue == "" {
		return fmt.Errorf("tasks: %q empty queue (set Def.Queue or Config.DefaultQueue)", d.Name)
	}
	d.Concurrency = normalizeConcurrency(d.Concurrency)
	if d.Cron != "" {
		if _, err := parseCron(d.Cron); err != nil {
			return fmt.Errorf("tasks: %q cron: %w", d.Name, err)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.defs[d.Name]; ok {
		return fmt.Errorf("tasks: duplicate %q", d.Name)
	}
	cp := d
	r.defs[d.Name] = &cp
	return nil
}

// SetProducer привязывает Producer к имени очереди (для Publish/Scheduler).
// Необязательно: Enqueue/Consume сами создадут Producer на rdb registry.
func (r *Registry) SetProducer(queue string, p *redisqueue.Producer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prods[queue] = p
}

func (r *Registry) ensureProducer(queue string) {
	if queue == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.prods[queue]; ok {
		return
	}
	r.prods[queue] = redisqueue.NewProducer(r.rdb, queue)
}

func (r *Registry) producer(queue string) (*redisqueue.Producer, error) {
	r.mu.RLock()
	p := r.prods[queue]
	r.mu.RUnlock()
	if p != nil {
		return p, nil
	}
	r.ensureProducer(queue)
	r.mu.RLock()
	defer r.mu.RUnlock()
	p = r.prods[queue]
	if p == nil {
		return nil, fmt.Errorf("tasks: no producer for queue %q", queue)
	}
	return p, nil
}

func (r *Registry) def(name string) (*Def, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.defs[name]
	return d, ok
}

// SyncCatalog выгружает схему в Redis (HSET catalog). Без отдельного names SET.
func (r *Registry) SyncCatalog(ctx context.Context) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := catalogKey(r.cfg.Namespace)
	pipe := r.rdb.Pipeline()
	pipe.Del(ctx, key)
	pipe.SAdd(ctx, namespacesKey(), r.cfg.Namespace)
	for _, d := range r.defs {
		ent := CatalogEntry{
			Name: d.Name, Queue: d.Queue, Cron: d.Cron,
			Concurrency: d.Concurrency, Args: d.Args,
		}
		b, err := json.Marshal(ent)
		if err != nil {
			return err
		}
		pipe.HSet(ctx, key, d.Name, b)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// CatalogForQueue — записи catalog с Queue == queue (для UI mode tasks).
func CatalogForQueue(ctx context.Context, rdb *redis.Client, namespace, queue string) ([]CatalogEntry, error) {
	raw, err := rdb.HGetAll(ctx, catalogKey(namespace)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]CatalogEntry, 0, len(raw))
	for _, v := range raw {
		var e CatalogEntry
		if json.Unmarshal([]byte(v), &e) != nil {
			continue
		}
		if queue == "" || e.Queue == queue {
			out = append(out, e)
		}
	}
	return out, nil
}

// ListNamespaces — SMEMBERS srq:tasks:namespaces; fallback SCAN catalog keys.
func ListNamespaces(ctx context.Context, rdb *redis.Client) ([]string, error) {
	ns, err := rdb.SMembers(ctx, namespacesKey()).Result()
	if err != nil {
		return nil, err
	}
	if len(ns) > 0 {
		return ns, nil
	}
	// старые деплои без namespaces SET
	var out []string
	iter := rdb.Scan(ctx, 0, "srq:tasks:*:catalog", 100).Iterator()
	for iter.Next(ctx) {
		k := iter.Val()
		rest := strings.TrimPrefix(k, "srq:tasks:")
		rest = strings.TrimSuffix(rest, ":catalog")
		if rest != "" && !strings.Contains(rest, ":") {
			out = append(out, rest)
		}
	}
	return out, iter.Err()
}

// BaseName — ID без суффикса bypass-lock (`name~hex`).
func BaseName(taskID string) string {
	if i := strings.IndexByte(taskID, '~'); i > 0 {
		return taskID[:i]
	}
	return taskID
}

// Dispatch вызывает handler по Task.ID (base name).
func (r *Registry) Dispatch(ctx context.Context, task *redisqueue.Task) error {
	if task == nil {
		return fmt.Errorf("tasks: nil task")
	}
	base := BaseName(task.ID)
	d, ok := r.def(base)
	if !ok {
		return fmt.Errorf("tasks: unknown %q", base)
	}
	args := argsFromTask(ctx, task, d.Args)
	_ = r.markRun(ctx, base, "running", "", "")
	inv := Invocation{Name: d.Name, Task: task, Args: args, RunID: task.ID, BaseName: base}
	err := d.Handle(ctx, inv)
	if err != nil {
		_ = r.markRun(ctx, base, "error", err.Error(), "")
		_ = r.flushPendingRerun(ctx, d)
		return err
	}
	_ = r.markRun(ctx, base, "ok", "", "")
	_ = r.flushPendingRerun(ctx, d)
	return nil
}

func (r *Registry) flushPendingRerun(ctx context.Context, d *Def) error {
	if d == nil || normalizeConcurrency(d.Concurrency) != ConcurrencyAfter {
		return nil
	}
	key := runKey(r.cfg.Namespace, d.Name)
	v, err := r.rdb.HGet(ctx, key, "pending_rerun").Result()
	if err != nil || v != "1" {
		return nil
	}
	_ = r.rdb.HDel(ctx, key, "pending_rerun").Err()
	// сразу после окончания текущей — не ждём следующий cron-тик
	_, err = r.Enqueue(ctx, d.Name, RunOptions{})
	return err
}

func argsFromTask(ctx context.Context, task *redisqueue.Task, schema []ArgDef) map[string]string {
	out := map[string]string{}
	for _, a := range schema {
		out[a.Name] = a.Default
	}
	if len(task.Payload) > 0 {
		var m map[string]string
		if json.Unmarshal(task.Payload, &m) == nil {
			for k, v := range m {
				out[k] = v
			}
		}
	}
	if hdrs, err := task.Headers(ctx); err == nil {
		for k, v := range hdrs {
			if strings.HasPrefix(k, "arg.") {
				out[strings.TrimPrefix(k, "arg.")] = v
			}
		}
	}
	return out
}

func (r *Registry) markRun(ctx context.Context, name, status, lastErr, progress string) error {
	key := runKey(r.cfg.Namespace, name)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fields := map[string]interface{}{
		"status": status,
	}
	if status == "running" {
		fields["started_at"] = now
		fields["finished_at"] = ""
		fields["last_error"] = ""
	}
	if status == "ok" || status == "error" {
		fields["finished_at"] = now
	}
	if lastErr != "" {
		fields["last_error"] = lastErr
	}
	if progress != "" {
		fields["progress"] = progress
	}
	return r.rdb.HSet(ctx, key, fields).Err()
}

// MirrorProgress пишет progress/step в run HASH (для UI после mid-flight).
func (r *Registry) MirrorProgress(ctx context.Context, name, stepRaw string) error {
	fields := map[string]interface{}{
		"progress": stepRaw,
		"step":     stepRaw,
		"status":   "running",
	}
	cp := redisqueue.ParseStepCheckpoint(stepRaw)
	if cp.Control == redisqueue.StepControlPause {
		cur, _ := r.rdb.HGet(ctx, runKey(r.cfg.Namespace, name), "paused_at").Result()
		if cur == "" {
			fields["paused_at"] = time.Now().UTC().Format(time.RFC3339Nano)
		}
	} else {
		fields["paused_at"] = ""
	}
	return r.rdb.HSet(ctx, runKey(r.cfg.Namespace, name), fields).Err()
}

// RunOptions — ручной/UI Publish.
type RunOptions struct {
	Args       map[string]string
	BypassLock bool // ~r{rand} — всегда новый ID
	ByOptions  bool // ~o{hash} — лок по набору args (игнор если BypassLock)
}

// Enqueue публикует задачу. Scheduler и UI Run.
// args для scheduler — только defaults из Def; UI может передать свои (не копятся в cron).
func (r *Registry) Enqueue(ctx context.Context, name string, opts RunOptions) (taskID string, err error) {
	d, ok := r.def(name)
	if !ok {
		return "", fmt.Errorf("tasks: unknown %q", name)
	}
	p, err := r.producer(d.Queue)
	if err != nil {
		return "", err
	}
	payloadArgs := map[string]string{}
	for _, a := range d.Args {
		payloadArgs[a.Name] = a.Default
	}
	for k, v := range opts.Args {
		payloadArgs[k] = v
	}
	id, err := BuildTaskID(name, payloadArgs, opts.BypassLock, opts.ByOptions)
	if err != nil {
		return "", err
	}
	body, _ := json.Marshal(payloadArgs)
	t := &redisqueue.Task{ID: id, Payload: body}
	if err := p.Publish(ctx, t); err != nil {
		return "", err
	}
	return id, nil
}

func defaultArgsMap(d *Def) map[string]string {
	m := map[string]string{}
	for _, a := range d.Args {
		m[a.Name] = a.Default
	}
	return m
}

func (r *Registry) setNextRun(ctx context.Context, name string, next time.Time) {
	_ = r.rdb.HSet(ctx, runKey(r.cfg.Namespace, name), "next_run_at", next.UTC().Format(time.RFC3339Nano)).Err()
}

func (r *Registry) scheduleTick(ctx context.Context, d *Def, now time.Time, nextDue map[string]time.Time) {
	sched, err := parseCron(d.Cron)
	if err != nil {
		return
	}
	due, ok := nextDue[d.Name]
	if !ok || due.IsZero() {
		due = sched.Next(now.Add(-time.Second))
		nextDue[d.Name] = due
		r.setNextRun(ctx, d.Name, due)
	}
	if now.Before(due) {
		return
	}
	// тик наступил
	next := sched.Next(now)
	nextDue[d.Name] = next
	r.setNextRun(ctx, d.Name, next)

	pol := normalizeConcurrency(d.Concurrency)
	_, err = r.Enqueue(ctx, d.Name, RunOptions{
		Args:       defaultArgsMap(d),
		BypassLock: pol == ConcurrencyAllow, // параллельный тик
	})
	if err == nil {
		return
	}
	if !isAlreadyExists(err) {
		return
	}
	// уже in-flight / в очереди
	if pol == ConcurrencyAfter {
		// флаг: как только текущий Dispatch закончится → Enqueue сразу (не ждать cron)
		_ = r.rdb.HSet(ctx, runKey(r.cfg.Namespace, d.Name), "pending_rerun", "1").Err()
	}
}

// StartScheduler — тик 1s; Cron из Def; payload = defaults схемы.
func (r *Registry) StartScheduler(parent context.Context) {
	if r.schedStop != nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	r.schedStop = cancel
	r.schedWG.Add(1)
	go func() {
		defer r.schedWG.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		nextDue := map[string]time.Time{}
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				r.mu.RLock()
				defs := make([]*Def, 0, len(r.defs))
				for _, d := range r.defs {
					if d.Cron != "" {
						defs = append(defs, d)
					}
				}
				r.mu.RUnlock()
				for _, d := range defs {
					r.scheduleTick(ctx, d, now, nextDue)
				}
			}
		}
	}()
}

// StopScheduler останавливает тикер.
func (r *Registry) StopScheduler() {
	if r.schedStop != nil {
		r.schedStop()
		r.schedWG.Wait()
		r.schedStop = nil
	}
}

// GetRun — last-run HASH.
func (r *Registry) GetRun(ctx context.Context, name string) (map[string]string, error) {
	return r.rdb.HGetAll(ctx, runKey(r.cfg.Namespace, name)).Result()
}

// Namespace возвращает ns.
func (r *Registry) Namespace() string { return r.cfg.Namespace }
