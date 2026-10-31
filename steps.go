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
//   - "name#curr/total@startedMs" / "name#curr/?@startedMs" — прогресс;
//   - "name#curr/total:cursor@startedMs" — прогресс + курсор продолжения;
//   - суффикс "!pause" / "!stop" — control.
type StepRunner struct {
	last           string
	skipUntil      string
	active         bool
	resumeProgress bool
	resumeCurr     int
	resumeTotal    int
	resumeUnknown  bool
	resumeStarted  time.Time
	resumeCursor   string

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
	name, curr, total, unk, started, cursor, hasProg, _ := parseStepValue(raw)
	if hasProg {
		// есть маркер прогресса → шаг не завершён
		s.skipUntil = name
		s.last = name
		s.resumeProgress = true
		s.resumeCurr = curr
		s.resumeTotal = total
		s.resumeUnknown = unk
		s.resumeStarted = started
		s.resumeCursor = cursor
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
	Cursor       string // opaque; продолжение с места остановки
	Started      time.Time
	HasProgress  bool
	Control      string // "", "pause", "stop"
}

// ParseStepCheckpoint разбирает "name" | "name#3/10@ms" | "name#3/?:cur@ms!pause".
func ParseStepCheckpoint(v string) StepCheckpoint {
	name, curr, total, unk, started, cursor, hp, ctrl := parseStepValue(v)
	return StepCheckpoint{
		Name: name, Curr: curr, Total: total, UnknownTotal: unk, Cursor: cursor,
		Started: started, HasProgress: hp, Control: ctrl,
	}
}

// sanitizeCursor — cursor в ключе без '@'/'!' (ломают @ms / !control).
func sanitizeCursor(c string) string {
	c = strings.TrimSpace(c)
	if c == "" {
		return ""
	}
	c = strings.ReplaceAll(c, "@", "")
	c = strings.ReplaceAll(c, "!", "")
	return c
}

// encodeStepValue — незавершённый прогресс (+ опциональный :cursor и !control).
// Имя без '#' (мета после первого #). Cursor без '@'/'!' (разделители @ms / !control).
func encodeStepValue(name string, curr, total int, unknownTotal bool, started time.Time, cursor, control string) string {
	if started.IsZero() {
		started = time.Now()
	}
	ms := started.UnixMilli()
	var mid string
	if unknownTotal || total <= 0 {
		mid = fmt.Sprintf("%d/?", curr)
	} else {
		mid = fmt.Sprintf("%d/%d", curr, total)
	}
	if c := sanitizeCursor(cursor); c != "" {
		mid += ":" + c
	}
	base := fmt.Sprintf("%s#%s@%d", name, mid, ms)
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

// parseStepValue: "name" | "name#3/10" | "name#3/?:cur@ms" | "name#3/10:cur@ms!pause"
// Без '#' — вся строка имя (можно '@', '!' и т.д.). Мета только после первого '#'.
func parseStepValue(v string) (name string, curr, total int, unknownTotal bool, started time.Time, cursor string, hasProgress bool, control string) {
	if v == "" {
		return "", 0, 0, false, time.Time{}, "", false, ""
	}
	i := strings.IndexByte(v, '#')
	if i < 0 {
		return v, 0, 0, false, time.Time{}, "", false, ""
	}
	name = v[:i]
	rest := v[i+1:]
	// rest: curr/total[:cursor][@ms][!control]
	bang := strings.LastIndexByte(rest, '!')
	at := strings.LastIndexByte(rest, '@')
	meta := rest
	if bang > at && bang >= 0 {
		control = normalizeStepControl(rest[bang+1:])
		meta = rest[:bang]
	}
	at = strings.LastIndexByte(meta, '@')
	if at >= 0 {
		if ms, err := strconv.ParseInt(meta[at+1:], 10, 64); err == nil && ms > 0 {
			started = time.UnixMilli(ms)
			meta = meta[:at]
		}
	}
	slash := strings.IndexByte(meta, '/')
	if slash < 0 {
		// «name#something» без curr/total — не progress, целиком как имя
		return v, 0, 0, false, time.Time{}, "", false, ""
	}
	currStr, afterSlash := meta[:slash], meta[slash+1:]
	c, err := strconv.Atoi(currStr)
	if err != nil || c < 0 {
		return v, 0, 0, false, time.Time{}, "", false, ""
	}
	totStr := afterSlash
	if colon := strings.IndexByte(afterSlash, ':'); colon >= 0 {
		totStr = afterSlash[:colon]
		cursor = afterSlash[colon+1:]
	}
	if totStr == "?" {
		return name, c, 0, true, started, cursor, true, control
	}
	t, err := strconv.Atoi(totStr)
	if err != nil || t < 0 {
		return v, 0, 0, false, time.Time{}, "", false, ""
	}
	return name, c, t, false, started, cursor, true, control
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

// LastCursor — cursor из mid-progress checkpoint (пустой если нет).
func (s *StepRunner) LastCursor() string {
	if s == nil || !s.resumeProgress {
		return ""
	}
	return s.resumeCursor
}

func (s *StepRunner) writeValue(ctx context.Context, value string) error {
	return s.rdb.Set(ctx, stepKey(s.queue, s.taskID), value, s.ttl).Err()
}

// stepProgressFlushScript — один RTT: пишет progress base, сохраняет !control из ключа, возвращает control.
// Отдельный ключ control не заводим: Ack уже чистит step; Lua атомарно не затирает pause/stop от UI.
var stepProgressFlushScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
local suffix = ''
if cur then
  local i = string.find(cur, '!', 1, true)
  if i then
    suffix = string.sub(cur, i)
  end
end
local val = ARGV[1] .. suffix
local ttl = tonumber(ARGV[2]) or 0
if ttl > 0 then
  redis.call('SET', KEYS[1], val, 'EX', ttl)
else
  redis.call('SET', KEYS[1], val)
end
if suffix == '' then
  return ''
end
return string.sub(suffix, 2)
`)

// stepControlScript — выставляет control, progress не трогает (один RTT).
var stepControlScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if not cur then
  return redis.error_reply('no step')
end
local body = cur
local i = string.find(cur, '!', 1, true)
if i then
  body = string.sub(cur, 1, i - 1)
end
local ctrl = ARGV[1]
local val = body
if ctrl ~= nil and ctrl ~= '' then
  val = body .. '!' .. ctrl
end
redis.call('SET', KEYS[1], val)
return val
`)

// Commit — шаг завершён (в ключе только имя, без #прогресса).
func (s *StepRunner) Commit(ctx context.Context, name string) error {
	if s == nil {
		return fmt.Errorf("steps: nil runner")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("step: empty name")
	}
	if strings.ContainsRune(name, '#') {
		return fmt.Errorf("step: name must not contain '#'")
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
	if strings.ContainsRune(name, '#') {
		return false, fmt.Errorf("step: name must not contain '#'")
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
	cursor := ""
	if s.resumeProgress && s.currentName == s.skipUntil {
		curr = s.resumeCurr
		unknown = s.resumeUnknown
		total = s.resumeTotal
		cursor = s.resumeCursor
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
		cursor:     cursor,
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
	cursor     string // opaque resume token (page / offset / last id)
	started    time.Time
	flushEvery time.Duration
	lastFlush  time.Time
	dirty      bool   // SetTotal / SetCursor — сбросить throttle
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

// SetCursor сохраняет opaque-курсор продолжения (page token / offset / last id).
// Символы '@' и '!' вырезаются. Запись в Redis — на следующем Advance/Flush.
func (p *StepProgress) SetCursor(c string) {
	if p == nil {
		return
	}
	p.cursor = sanitizeCursor(c)
	p.dirty = true
}

// Cursor — текущий курсор (в т.ч. после resume из ключа).
func (p *StepProgress) Cursor() string {
	if p == nil {
		return ""
	}
	return p.cursor
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
	// base без control — Lua подставит !pause/!stop из текущего ключа
	base := encodeStepValue(p.name, p.current, p.total, p.unknown, p.started, p.cursor, StepControlNone)
	ttlSec := int64(0)
	if p.runner.ttl > 0 {
		ttlSec = int64(p.runner.ttl / time.Second)
		if ttlSec < 1 {
			ttlSec = 1
		}
	}
	ctrl, err := stepProgressFlushScript.Run(ctx, p.runner.rdb, []string{key}, base, ttlSec).Text()
	if err != nil {
		return fmt.Errorf("step progress %q: %w", p.name, err)
	}
	p.control = normalizeStepControl(ctrl)
	p.runner.last = p.name
	p.runner.resumeProgress = true
	p.runner.resumeCurr = p.current
	p.runner.resumeTotal = p.total
	p.runner.resumeUnknown = p.unknown
	p.runner.resumeStarted = p.started
	p.runner.resumeCursor = p.cursor
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
		_, _, _, _, _, _, _, ctrl := parseStepValue(raw)
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
	_, err := stepControlScript.Run(ctx, rdb, []string{key}, control).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no step") {
			return fmt.Errorf("steps: no step for %s/%s", queueName, taskID)
		}
		return err
	}
	return nil
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
