package redisqueue

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func stepKey(queueName, taskID string) string {
	return "queue:" + queueName + ":step:" + taskID
}

// StepFunc — тело одного шага recovery (Do).
type StepFunc func(ctx context.Context) error

// StepsOptions — опции NewSteps / NewStepsRaw.
type StepsOptions struct {
	// TTL — EXPIRE на ключ step при каждом Commit/Advance. 0 = без TTL.
	TTL time.Duration
}

// StepProgressConfig — опции Progress (имя шага берётся из Begin/Do).
type StepProgressConfig struct {
	// FlushEvery — как часто писать в Redis при Advance; 0 = на каждый Advance.
	FlushEvery time.Duration
}

// StepRunner — checkpoint по строковому step.
//
// Значение ключа:
//   - "name" — шаг завершён;
//   - "name#curr/total@startedMs" или "name#curr/?@startedMs" — шаг в процессе (маркер прогресса).
type StepRunner struct {
	last           string
	skipUntil      string
	active         bool
	resumeProgress bool
	resumeCurr     int
	resumeTotal    int
	resumeUnknown  bool
	resumeStarted  time.Time

	currentName string // активный Begin/Do
	bar         *StepProgress

	rdb    *redis.Client
	queue  string
	taskID string
	ttl    time.Duration
}

// NewSteps создаёт runner из Task после Get/Consume.
func NewSteps(ctx context.Context, task *Task, opts ...StepsOptions) (*StepRunner, error) {
	if task == nil {
		return nil, fmt.Errorf("steps: task is nil")
	}
	if task.bindRedis == nil || task.bindQueue == "" {
		return nil, fmt.Errorf("steps: task has no queue bind (use task from Consumer.Get / Consume, or NewStepsRaw)")
	}
	var o StepsOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	return newSteps(ctx, task.bindRedis, task.bindQueue, task.ID, o)
}

// NewStepsRaw — без Task: явные redis/queue/id.
func NewStepsRaw(ctx context.Context, rdb *redis.Client, queueName, taskID string, opts ...StepsOptions) (*StepRunner, error) {
	var o StepsOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	return newSteps(ctx, rdb, queueName, taskID, o)
}

func newSteps(ctx context.Context, rdb *redis.Client, queueName, taskID string, opts StepsOptions) (*StepRunner, error) {
	if rdb == nil {
		return nil, fmt.Errorf("steps: redis client is nil")
	}
	if queueName == "" || taskID == "" {
		return nil, fmt.Errorf("steps: queue and task id required")
	}
	raw := ""
	val, err := rdb.Get(ctx, stepKey(queueName, taskID)).Result()
	if err == nil {
		raw = val
	} else if err != redis.Nil {
		return nil, fmt.Errorf("steps: load checkpoint: %w", err)
	}

	s := &StepRunner{rdb: rdb, queue: queueName, taskID: taskID, ttl: opts.TTL}
	if raw == "" {
		s.active = true
		return s, nil
	}
	name, curr, total, unk, started, hasProg, _ := parseStepValue(raw)
	if hasProg {
		// есть маркер прогресса → шаг не завершён
		s.skipUntil = name
		s.last = name
		s.resumeProgress = true
		s.resumeCurr = curr
		s.resumeTotal = total
		s.resumeUnknown = unk
		s.resumeStarted = started
		s.active = false
		return s, nil
	}
	s.last = name
	s.skipUntil = name
	s.active = false
	return s, nil
}

// StepControl — внешнее управление процессом (пишется в тот же step-ключ).
const (
	StepControlNone  = ""
	StepControlPause = "pause"
	StepControlStop  = "stop"
)

// ErrStepStopped — Advance/Flush увидел !stop в step-ключе.
var ErrStepStopped = fmt.Errorf("steps: stopped")

// StepCheckpoint — разобранное значение step-ключа.
type StepCheckpoint struct {
	Name         string
	Curr, Total  int
	UnknownTotal bool
	Started      time.Time
	HasProgress  bool
	Control      string // "", "pause", "stop"
}

// ParseStepCheckpoint разбирает "name" | "name#3/10@ms" | "name#3/10@ms!pause".
func ParseStepCheckpoint(v string) StepCheckpoint {
	name, curr, total, unk, started, hp, ctrl := parseStepValue(v)
	return StepCheckpoint{
		Name: name, Curr: curr, Total: total, UnknownTotal: unk,
		Started: started, HasProgress: hp, Control: ctrl,
	}
}

// encodeStepValue — незавершённый прогресс (+ опциональный !control). Имя без '#'/'@'/'!'.
func encodeStepValue(name string, curr, total int, unknownTotal bool, started time.Time, control string) string {
	if started.IsZero() {
		started = time.Now()
	}
	ms := started.UnixMilli()
	var base string
	if unknownTotal || total <= 0 {
		base = fmt.Sprintf("%s#%d/?@%d", name, curr, ms)
	} else {
		base = fmt.Sprintf("%s#%d/%d@%d", name, curr, total, ms)
	}
	control = normalizeStepControl(control)
	if control != "" {
		return base + "!" + control
	}
	return base
}

func normalizeStepControl(c string) string {
	switch strings.ToLower(strings.TrimSpace(c)) {
	case StepControlPause:
		return StepControlPause
	case StepControlStop:
		return StepControlStop
	default:
		return StepControlNone
	}
}

// parseStepValue: "name" | "name#3/10" | "name#3/?@ms" | "name#3/10@ms!pause"
func parseStepValue(v string) (name string, curr, total int, unknownTotal bool, started time.Time, hasProgress bool, control string) {
	if v == "" {
		return "", 0, 0, false, time.Time{}, false, ""
	}
	// control suffix: …@ms!pause
	bang := strings.LastIndexByte(v, '!')
	at := strings.LastIndexByte(v, '@')
	body := v
	if bang > at && bang > 0 {
		control = normalizeStepControl(v[bang+1:])
		body = v[:bang]
	}
	at = strings.LastIndexByte(body, '@')
	if at > 0 {
		if ms, err := strconv.ParseInt(body[at+1:], 10, 64); err == nil && ms > 0 {
			started = time.UnixMilli(ms)
			body = body[:at]
		}
	}
	i := strings.LastIndexByte(body, '#')
	if i < 0 {
		return body, 0, 0, false, time.Time{}, false, control
	}
	rest := body[i+1:]
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return body, 0, 0, false, time.Time{}, false, control
	}
	currStr, totStr := rest[:slash], rest[slash+1:]
	c, err := strconv.Atoi(currStr)
	if err != nil || c < 0 {
		return body, 0, 0, false, time.Time{}, false, control
	}
	name = body[:i]
	if totStr == "?" {
		return name, c, 0, true, started, true, control
	}
	t, err := strconv.Atoi(totStr)
	if err != nil || t < 0 {
		return body, 0, 0, false, time.Time{}, false, control
	}
	return name, c, t, false, started, true, control
}

// Last — имя checkpoint без суффикса прогресса.
func (s *StepRunner) Last() string {
	if s == nil {
		return ""
	}
	return s.last
}

// LastProgress — curr/total/started при незавершённом баре в ключе.
func (s *StepRunner) LastProgress() (curr, total int, unknownTotal bool, started time.Time, ok bool) {
	if s == nil || !s.resumeProgress {
		return 0, 0, false, time.Time{}, false
	}
	return s.resumeCurr, s.resumeTotal, s.resumeUnknown, s.resumeStarted, true
}

func (s *StepRunner) writeValue(ctx context.Context, value string) error {
	return s.rdb.Set(ctx, stepKey(s.queue, s.taskID), value, s.ttl).Err()
}

// writeValueGet — SET value GET: атомарно пишет и возвращает предыдущее значение ("" если ключа не было).
func (s *StepRunner) writeValueGet(ctx context.Context, value string) (old string, err error) {
	args := redis.SetArgs{Get: true}
	if s.ttl > 0 {
		args.TTL = s.ttl
	}
	old, err = s.rdb.SetArgs(ctx, stepKey(s.queue, s.taskID), value, args).Result()
	if err == redis.Nil {
		return "", nil
	}
	return old, err
}

// Commit — шаг завершён (в ключе только имя, без #прогресса).
func (s *StepRunner) Commit(ctx context.Context, name string) error {
	if s == nil {
		return fmt.Errorf("steps: nil runner")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("step: empty name")
	}
	if err := s.writeValue(ctx, name); err != nil {
		return fmt.Errorf("step commit %q: %w", name, err)
	}
	s.last = name
	s.resumeProgress = false
	s.currentName = ""
	s.bar = nil
	return nil
}

// Begin входит в шаг. ok=false — шаг уже завершён, тело пропускаем.
// Имя задаётся один раз; Progress() берёт его отсюда.
func (s *StepRunner) Begin(ctx context.Context, name string) (ok bool, err error) {
	if s == nil {
		return false, fmt.Errorf("steps: nil runner")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return false, fmt.Errorf("step: empty name")
	}
	if s.currentName != "" {
		return false, fmt.Errorf("steps: already in step %q (End first)", s.currentName)
	}

	if !s.active {
		if name != s.skipUntil {
			return false, nil
		}
		s.active = true
		if !s.resumeProgress {
			// завершённый checkpoint с этим именем
			return false, nil
		}
		// mid-progress — выполняем снова
	} else if name == s.last && !s.resumeProgress {
		return false, nil
	}

	s.currentName = name
	return true, nil
}

// End завершает текущий Begin-шаг (Commit имени без прогресса).
func (s *StepRunner) End(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("steps: nil runner")
	}
	if s.currentName == "" {
		return fmt.Errorf("steps: End without Begin")
	}
	name := s.currentName
	if s.bar != nil {
		_ = s.bar.flush(ctx)
		s.bar = nil
	}
	return s.Commit(ctx, name)
}

// Progress — bar для текущего Begin/Do шага (имя не повторяем).
func (s *StepRunner) Progress(ctx context.Context, cfg StepProgressConfig) (*StepProgress, error) {
	if s == nil {
		return nil, fmt.Errorf("steps: nil runner")
	}
	if s.currentName == "" {
		return nil, fmt.Errorf("steps: Progress outside Begin/Do")
	}
	if s.bar != nil {
		return s.bar, nil
	}

	curr, total, unknown := 0, 0, true
	started := time.Now()
	if s.resumeProgress && s.currentName == s.skipUntil {
		curr = s.resumeCurr
		unknown = s.resumeUnknown
		total = s.resumeTotal
		if !s.resumeStarted.IsZero() {
			started = s.resumeStarted
		}
	}

	p := &StepProgress{
		runner:     s,
		name:       s.currentName,
		total:      total,
		unknown:    unknown,
		current:    curr,
		started:    started,
		flushEvery: cfg.FlushEvery,
	}
	s.bar = p
	// сразу зафиксировать старт в ключе (шаг начат, ещё не завершён)
	if err := p.flush(ctx); err != nil {
		s.bar = nil
		return nil, err
	}
	return p, nil
}

// Do — Begin + fn + End. Внутри fn: bar, _ := s.Progress(ctx, cfg).
func (s *StepRunner) Do(ctx context.Context, name string, fn StepFunc) error {
	if fn == nil {
		return fmt.Errorf("step %q: nil func", name)
	}
	ok, err := s.Begin(ctx, name)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if err := fn(ctx); err != nil {
		// прогресс остаётся в ключе для resume
		s.currentName = ""
		s.bar = nil
		// active остаётся true — следующие Do в этой попытке могут идти;
		// но skipUntil/resume уже в Redis. На следующем NewSteps подхватят mid-bar.
		// Чтобы следующие Do в том же handler не ломались после ошибки mid-step —
		// оставляем active=true, currentName cleared; resumeProgress из Redis на след. Get.
		return err
	}
	return s.End(ctx)
}

// StepProgress — счётчик внутри текущего шага.
type StepProgress struct {
	runner     *StepRunner
	name       string
	total      int
	unknown    bool
	current    int
	started    time.Time
	flushEvery time.Duration
	lastFlush  time.Time
	dirty      bool   // SetTotal / смена total — сбросить throttle
	control    string // из step-ключа (!pause / !stop)
}

func (p *StepProgress) Current() int {
	if p == nil {
		return 0
	}
	return p.current
}

func (p *StepProgress) Total() int {
	if p == nil || p.unknown {
		return 0
	}
	return p.total
}

// SetTotal задаёт максимум (0 → «?»). Можно вызвать после Progress.
func (p *StepProgress) SetTotal(n int) {
	if p == nil {
		return
	}
	if n <= 0 {
		p.unknown = true
		p.total = 0
	} else {
		p.unknown = false
		p.total = n
	}
	p.dirty = true
}

// StartedAt — время старта бара (сохраняется в ключе при resume).
func (p *StepProgress) StartedAt() time.Time {
	if p == nil {
		return time.Time{}
	}
	return p.started
}

// Elapsed — сколько уже идёт шаг.
func (p *StepProgress) Elapsed() time.Duration {
	if p == nil || p.started.IsZero() {
		return 0
	}
	return time.Since(p.started)
}

// ETA — оценка до конца по current/total; 0 если total неизвестен или current=0.
func (p *StepProgress) ETA() time.Duration {
	if p == nil || p.unknown || p.total <= 0 || p.current <= 0 {
		return 0
	}
	if p.current >= p.total {
		return 0
	}
	per := p.Elapsed() / time.Duration(p.current)
	return per * time.Duration(p.total-p.current)
}

// Control — текущий control из последнего flush (pause/stop/"").
func (p *StepProgress) Control() string {
	if p == nil {
		return ""
	}
	return p.control
}

// Advance добавляет n (n<1 → 1) и при необходимости пишет в Redis.
// При !pause в ключе — ждёт resume/stop; при !stop — ErrStepStopped.
func (p *StepProgress) Advance(ctx context.Context, n int) error {
	if p == nil || p.runner == nil {
		return fmt.Errorf("steps: nil progress")
	}
	if err := p.applyControl(ctx); err != nil {
		return err
	}
	if n < 1 {
		n = 1
	}
	p.current += n
	if p.dirty || p.flushEvery <= 0 || p.lastFlush.IsZero() || time.Since(p.lastFlush) >= p.flushEvery {
		p.dirty = false
		return p.flush(ctx)
	}
	return nil
}

// Flush принудительно пишет прогресс в Redis.
func (p *StepProgress) Flush(ctx context.Context) error {
	if p == nil || p.runner == nil {
		return fmt.Errorf("steps: nil progress")
	}
	return p.flush(ctx)
}

func (p *StepProgress) flush(ctx context.Context) error {
	if p.started.IsZero() {
		p.started = time.Now()
	}
	key := stepKey(p.runner.queue, p.runner.taskID)
	if cur, err := p.runner.rdb.Get(ctx, key).Result(); err == nil {
		_, _, _, _, _, _, ctrl := parseStepValue(cur)
		p.control = ctrl
	} else if err != redis.Nil {
		return fmt.Errorf("step progress %q: %w", p.name, err)
	}
	val := encodeStepValue(p.name, p.current, p.total, p.unknown, p.started, p.control)
	prev, err := p.runner.writeValueGet(ctx, val)
	if err != nil {
		return fmt.Errorf("step progress %q: %w", p.name, err)
	}
	if prev != "" {
		_, _, _, _, _, _, prevCtrl := parseStepValue(prev)
		if prevCtrl != p.control {
			// UI успел сменить control между GET и SET — восстановим
			p.control = prevCtrl
			val = encodeStepValue(p.name, p.current, p.total, p.unknown, p.started, p.control)
			if err := p.runner.writeValue(ctx, val); err != nil {
				return fmt.Errorf("step progress %q: %w", p.name, err)
			}
		}
	}
	p.runner.last = p.name
	p.runner.resumeProgress = true
	p.runner.resumeCurr = p.current
	p.runner.resumeTotal = p.total
	p.runner.resumeUnknown = p.unknown
	p.runner.resumeStarted = p.started
	p.lastFlush = time.Now()
	return p.applyControl(ctx)
}

func (p *StepProgress) applyControl(ctx context.Context) error {
	for {
		switch p.control {
		case StepControlStop:
			return ErrStepStopped
		case StepControlPause:
			if err := p.waitWhilePaused(ctx); err != nil {
				return err
			}
			continue
		default:
			return nil
		}
	}
}

func (p *StepProgress) waitWhilePaused(ctx context.Context) error {
	key := stepKey(p.runner.queue, p.runner.taskID)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		raw, err := p.runner.rdb.Get(ctx, key).Result()
		if err == redis.Nil {
			p.control = StepControlNone
			return nil
		}
		if err != nil {
			return fmt.Errorf("step progress pause poll: %w", err)
		}
		_, _, _, _, _, _, ctrl := parseStepValue(raw)
		p.control = ctrl
		switch ctrl {
		case StepControlStop:
			return ErrStepStopped
		case StepControlPause:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		default:
			return nil
		}
	}
}

// SetStepControl — UI/admin: выставить pause|stop|"" на существующий step (progress сохраняется).
func SetStepControl(ctx context.Context, rdb *redis.Client, queueName, taskID, control string) error {
	if rdb == nil {
		return fmt.Errorf("steps: redis client is nil")
	}
	control = normalizeStepControl(control)
	key := stepKey(queueName, taskID)
	raw, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return fmt.Errorf("steps: no step for %s/%s", queueName, taskID)
	}
	if err != nil {
		return err
	}
	name, curr, total, unk, started, hp, _ := parseStepValue(raw)
	if !hp {
		// завершённый checkpoint — некуда вешать control
		return fmt.Errorf("steps: step %q has no progress marker", name)
	}
	val := encodeStepValue(name, curr, total, unk, started, control)
	return rdb.Set(ctx, key, val, 0).Err()
}

// Release удаляет checkpoint вручную.
func (s *StepRunner) Release(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if err := s.rdb.Del(ctx, stepKey(s.queue, s.taskID)).Err(); err != nil {
		return fmt.Errorf("step release: %w", err)
	}
	s.last = ""
	s.skipUntil = ""
	s.active = true
	s.resumeProgress = false
	s.currentName = ""
	s.bar = nil
	return nil
}
