package redisqueue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RejectWithDelay — ошибка, при которой Consume делает Reject с теми же опциями.
// Delay / BlockGroups / BlockTags / DeadLetter — через RejectOption.
// Для ordered-партиций (!) дополнительно ставится partition в blocked.
type RejectWithDelay struct {
	Err           error
	Delay         float64
	BlockGroups   []string
	BlockTags     []string
	DeadLetter    bool
	DeadLetterMax int // ёмкость DLQ; -1 = взять Consumer.SetDLQMaxSize; 0 = без лимита
}

func (e *RejectWithDelay) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return "reject with delay"
}

func (e *RejectWithDelay) Unwrap() error {
	return e.Err
}

// rejectConfig — накопитель для Reject(...RejectOption) и NewRejectWithDelay.
type rejectConfig struct {
	delay       float64
	blockGroups []string
	blockTags   []string
	deadLetter  bool
	dlqMax      int // -1 = consumer default; 0 = unlimited; >0 = cap
}

// RejectOption — опция Reject / NewRejectWithDelay (порядок любой).
type RejectOption func(*rejectConfig)

// WithDelay — секунды до повтора (дробные OK); для BlockTags ещё и TTL block:{id}.
func WithDelay(seconds float64) RejectOption {
	return func(c *rejectConfig) {
		if seconds < 0 {
			seconds = 0
		}
		c.delay = seconds
	}
}

// WithBlockGroups — парковка групп на Delay (score unlockAt в groups).
func WithBlockGroups(groups ...string) RejectOption {
	return func(c *rejectConfig) {
		c.blockGroups = append([]string(nil), groups...)
	}
}

// WithBlockTags — race!b! ids (или полные теги): SET block:{id} wait на Delay.
func WithBlockTags(ids ...string) RejectOption {
	return func(c *rejectConfig) {
		c.blockTags = append([]string(nil), ids...)
	}
}

// WithDeadLetter — вместо возврата в очередь положить задачу в queue:{name}:dlq.
// Meta сохраняется для Admin.RetryDLQ; srq:queues −1.
//
// maxSize (опционально):
//   - без аргумента — лимит из Consumer.SetDLQMaxSize (0 = без лимита);
//   - 0 — явно без лимита;
//   - N>0 — после ZADD держать ZCARD(dlq) ≤ N (вытеснение самых старых + meta).
func WithDeadLetter(maxSize ...int) RejectOption {
	return func(c *rejectConfig) {
		c.deadLetter = true
		if len(maxSize) == 0 {
			c.dlqMax = -1
			return
		}
		if maxSize[0] <= 0 {
			c.dlqMax = 0
			return
		}
		c.dlqMax = maxSize[0]
	}
}

func applyRejectOptions(opts []RejectOption) rejectConfig {
	cfg := rejectConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// NewRejectWithDelay создаёт ошибку для Consume → Reject.
// Пример: NewRejectWithDelay(err, WithDelay(2), WithDeadLetter()).
func NewRejectWithDelay(err error, opts ...RejectOption) *RejectWithDelay {
	cfg := applyRejectOptions(opts)
	return &RejectWithDelay{
		Err:           err,
		Delay:         cfg.delay,
		BlockGroups:   cfg.blockGroups,
		BlockTags:     cfg.blockTags,
		DeadLetter:    cfg.deadLetter,
		DeadLetterMax: cfg.dlqMax,
	}
}

// HandlerFunc — обработчик задачи для Consume.
type HandlerFunc func(*Task) error

// HandlerMiddleware оборачивает handler (логирование, Sentry, метрики и т.п.).
// Первый Use — внешний слой: видит ошибку/панику внутреннего handler как обычный error.
type HandlerMiddleware func(next HandlerFunc) HandlerFunc

// HandlerPanicError — паника внутри handler (или middleware), перехваченная Consume.
// Контракт тот же, что у обычной ошибки handler: задача уходит в Reject, цикл продолжается.
type HandlerPanicError struct {
	Value any
	Stack []byte
}

func (e *HandlerPanicError) Error() string {
	return fmt.Sprintf("handler panic: %v", e.Value)
}

// callHandler вызывает handler и переводит панику в HandlerPanicError.
func callHandler(handler HandlerFunc, task *Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &HandlerPanicError{Value: r, Stack: debug.Stack()}
		}
	}()
	return handler(task)
}

// applyMiddlewares строит цепочку: recover вокруг user handler, затем middleware снаружи.
func applyMiddlewares(middlewares []HandlerMiddleware, handler HandlerFunc) HandlerFunc {
	next := HandlerFunc(func(task *Task) error {
		return callHandler(handler, task)
	})
	for i := len(middlewares) - 1; i >= 0; i-- {
		next = middlewares[i](next)
	}
	return next
}

// ErrTasksAlreadyExist возникает, когда одна или несколько задач уже существуют.
type ErrTasksAlreadyExist struct {
	TaskIDs []string
}

func (e *ErrTasksAlreadyExist) Error() string {
	return fmt.Sprintf("%d tasks already exist: %v", len(e.TaskIDs), e.TaskIDs)
}

// generateConsumerID генерирует уникальный ID консьюмера: UnixNano + 8 случайных байт в hex.
// Коллизии практически исключены при распределённом запуске воркеров.
func generateConsumerID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// fallback на менее случайный, но всё же уникальный ID
		b = []byte(fmt.Sprintf("%08x", time.Now().UnixNano()%0xFFFFFFFF))
	}
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b))
}

// Task задача в очереди
type Task struct {
	ID                string    `json:"id"`
	Partition         string    `json:"partition,omitempty"`
	PartitionPriority int       `json:"partitionPriority,omitempty"` // порядок внутри партиции (больше → раньше)
	QueuePriority     int       `json:"queuePriority,omitempty"`     // порядок по очереди; 0 = legacy-ключи без qp-префикса
	Payload           []byte    `json:"-"`
	Scheduled         time.Time `json:"scheduled"`
	Tags              []string  `json:"tags,omitempty"`        // теги для индексации / массового удаления
	Groups            []string  `json:"groups,omitempty"`      // rate-limit / обход Get (пустые → DefaultGroup)
	RejectCount       int       `json:"rejectCount,omitempty"` // кол-во reject для расчёта задержки
	// Shared — prepare-once blob’ы с Get (ключ = id из race!s!{id}). Нил/пусто = вы лидер prepare.
	Shared map[string][]byte `json:"-"`

	// bind* — клиент/очередь/consumer после Get (NewSteps, NewShared; шард верный).
	bindRedis           *redis.Client
	bindQueue           string
	bindConsumerID      string
	bindConcurrencyTags bool

	// ackShared / ackOpen / ackSharedTTL — что Consume передаст в Ack.
	ackShared    map[string][]byte
	ackOpen      []string
	ackSharedTTL time.Duration
}

// SetAckShared — Shared, который Consume приложит к авто-Ack (ключ = id race!s!).
// ttl опционален (один аргумент); 0/нет → DefaultSharedTTL на стороне Ack.
func (t *Task) SetAckShared(shared map[string][]byte, ttl ...time.Duration) {
	t.ackShared = shared
	if len(ttl) > 0 {
		t.ackSharedTTL = ttl[0]
	} else {
		t.ackSharedTTL = 0
	}
}

// SetAckOpenBlocks — race!b! ids, которые Consume откроет (ready) на авто-Ack.
func (t *Task) SetAckOpenBlocks(ids ...string) {
	t.ackOpen = append([]string(nil), ids...)
}

// encodeTags сериализует теги в строку для Lua add-скрипта.
// Пустые отбрасываются, дубликаты схлопываются; TagSeparator внутри тега запрещён.
func encodeTags(tags []string) (string, error) {
	return encodeSeparated(tags, "tag")
}

// encodeGroups сериализует группы (тот же разделитель и правила, что у тегов).
func encodeGroups(groups []string) (string, error) {
	return encodeSeparated(groups, "group")
}

func encodeSeparated(items []string, kind string) (string, error) {
	if len(items) == 0 {
		return "", nil
	}
	seen := make(map[string]struct{}, len(items))
	parts := make([]string, 0, len(items))
	for _, item := range items {
		if item == "" {
			continue
		}
		if strings.Contains(item, TagSeparator) {
			return "", fmt.Errorf("%s %q contains forbidden separator %q", kind, item, TagSeparator)
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		parts = append(parts, item)
	}
	return strings.Join(parts, TagSeparator), nil
}

// Producer отправляет задачи в очередь
type Producer struct {
	redis            *redis.Client
	queueName        string
	compression      Compression
	compressMinBytes int // 0 → defaultCompressMinBytes при Compression != None
	router           *ShardRouter
	ownRouter        bool
	shardPubs        []*Producer // по одному на шард; без вложенного router
}

// NewProducer создает нового продюсера. Для нескольких Redis — SetShards.
func NewProducer(redisClient *redis.Client, queueName string) *Producer {
	p := &Producer{
		redis:     redisClient,
		queueName: queueName,
	}
	_ = EnsureQueue(context.Background(), redisClient, queueName)
	return p
}

// SetCompression — сжатие payload на всех Publish (F6). CompressionNone (дефолт) — без оверхеда.
// Consumer/Admin декодируют по маркеру автоматически.
func (p *Producer) SetCompression(c Compression) {
	p.compression = c
	for _, sp := range p.shardPubs {
		sp.compression = c
	}
}

// SetCompressMinBytes — сжимать только payload ≥ n байт (дефолт 1024). 0 = сбросить на дефолт.
func (p *Producer) SetCompressMinBytes(n int) {
	p.compressMinBytes = n
	for _, sp := range p.shardPubs {
		sp.compressMinBytes = n
	}
}

// Publish добавляет одну или несколько задач в очередь атомарно, сохраняя порядок.
// Без backpressure — см. PublishWith + WithTagBackpressure / WithTagLimits.
func (p *Producer) Publish(ctx context.Context, tasks ...*Task) error {
	return p.PublishWith(ctx, nil, tasks...)
}

// PublishWith — как Publish, плюс opt-in опции (backpressure по тегам; при шардинге — WithShardKey / WithShardName).
func (p *Producer) PublishWith(ctx context.Context, opts []PublishOption, tasks ...*Task) error {
	if len(tasks) == 0 {
		return nil
	}
	if p.router != nil {
		return p.publishSharded(ctx, opts, tasks)
	}
	return p.publishLocal(ctx, opts, tasks)
}

func (p *Producer) publishLocal(ctx context.Context, opts []PublishOption, tasks []*Task) error {
	cfg := &publishConfig{compression: p.compression, compressMinBytes: p.compressMinBytes}
	cfg.apply(opts)
	if err := cfg.validate(); err != nil {
		return err
	}
	if cfg.journalName != "" && cfg.journalCursor == "" {
		return fmt.Errorf("WithJournal: cursor required")
	}

	bp := cfg.bpArgs()
	args := make([]interface{}, 0, 6+len(bp)+len(tasks)*8)
	if cfg.journalName != "" {
		// Lua: "jn", cursorKey, newCursor, queueName, taskCount, bpCount, ...
		args = append(args, journalARGVMarker, JournalCursorKey(cfg.journalName), cfg.journalCursor,
			p.queueName, len(tasks))
	} else {
		args = append(args, p.queueName, len(tasks))
	}
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
		// []byte в ARGV — binary-safe RESP bulk string (без string() копирования смысла)
		args = append(args, task.ID, task.Partition, task.PartitionPriority, task.QueuePriority, scheduled, payload, tags, groups)
	}

	script := getAddScript()
	result, err := script.Run(ctx, p.redis, []string{}, args...).Result()
	if err != nil {
		if isJournalCursorRewind(err) {
			return ErrJournalCursorRewind
		}
		return fmt.Errorf("failed to publish tasks: %w", err)
	}

	notAddedIndices, ok := result.([]interface{})
	if !ok {
		return fmt.Errorf("failed to get response: %w", err)
	}

	if len(notAddedIndices) == 0 {
		return nil
	}

	// Sentinel backpressure: {'!', 'backpressure', tag, count, limit}
	if s, _ := notAddedIndices[0].(string); s == "!" {
		return parseBackpressureResult(notAddedIndices)
	}

	notAddedIDs := make([]string, 0, len(notAddedIndices))
	for _, idxInterface := range notAddedIndices {
		idx, ok := idxInterface.(int64)
		if !ok {
			continue
		}
		if int(idx) < len(tasks) {
			notAddedIDs = append(notAddedIDs, tasks[idx].ID)
		}
	}

	return &ErrTasksAlreadyExist{TaskIDs: notAddedIDs}
}

func parseBackpressureResult(res []interface{}) error {
	if len(res) < 5 {
		return fmt.Errorf("backpressure: malformed script result")
	}
	tag, _ := res[2].(string)
	count, _ := strconv.ParseInt(fmt.Sprint(res[3]), 10, 64)
	limit, _ := strconv.ParseInt(fmt.Sprint(res[4]), 10, 64)
	return &ErrBackpressure{Tag: tag, Count: count, Limit: limit}
}

// Lock добавляет одну или несколько блокировок
func (p *Producer) Lock(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if p.router != nil {
		return p.lockSharded(ctx, ids)
	}
	return p.lockLocal(ctx, ids)
}

func (p *Producer) lockLocal(ctx context.Context, ids []string) error {
	args := make([]interface{}, 0, 2+len(ids))
	args = append(args, p.queueName, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}

	script := getLockScript()
	result, err := script.Run(ctx, p.redis, []string{}, args...).Result()
	if err != nil {
		return fmt.Errorf("failed to publish tasks: %w", err)
	}

	// Скрипт возвращает массив индексов задач, которые не были добавлены
	notAddedIndices, ok := result.([]interface{})
	if !ok {
		return fmt.Errorf("failed to get response: %w", err)
	}

	if len(notAddedIndices) == 0 {
		return nil
	}

	// Собираем ID задач по возвращённым индексам
	notAddedIDs := make([]string, 0, len(notAddedIndices))
	for _, idxInterface := range notAddedIndices {
		idx, ok := idxInterface.(int64) // Lua number -> int64
		if !ok {
			continue
		}
		if int(idx) < len(ids) {
			notAddedIDs = append(notAddedIDs, ids[idx])
		}
	}

	return &ErrTasksAlreadyExist{TaskIDs: notAddedIDs}
}

// Consumer получает и обрабатывает задачи из очереди
type Consumer struct {
	redis                       *redis.Client
	queueName                   string
	consumerID                  string
	pollInterval                time.Duration
	prefetchCount               int
	checkDeadConsumerLocksOnGet bool
	idempotencyTtl              time.Duration
	rateLimit                   *RateLimitConfig
	concurrencyTags             bool // F5: проверка race! в Get / release в Ack·Reject
	samePartitionBatch          bool
	extendGroupBlocksOnly       bool
	dlqMaxSize                  int // дефолт для WithDeadLetter(); 0 = без лимита
	middlewares                 []HandlerMiddleware
	stopPing                    chan struct{}
	stats                       *StatsSink
	statsOwn                    bool // true — Close останавливает stats loop
	journalAck                  func(ctx context.Context, taskID string) error
	afterAck                    func(ctx context.Context, taskID string) error
	emergencyResume             bool
	faultAt                     time.Time
}

// NewConsumer создает нового консьюмера и запускает ping горутину.
// consumerID — идентификатор консьюмера; если пустая строка — генерируется автоматически.
func NewConsumer(redisClient *redis.Client, queueName string, consumerID string) *Consumer {
	return newConsumer(redisClient, queueName, consumerID, true)
}

// newConsumer создает консьюмера. startPing — запускать ли pingLoop (для тестов можно false).
func newConsumer(redisClient *redis.Client, queueName string, consumerID string, startPing bool) *Consumer {
	if consumerID == "" {
		consumerID = generateConsumerID()
	}
	c := &Consumer{
		redis:                       redisClient,
		queueName:                   queueName,
		consumerID:                  consumerID,
		pollInterval:                1 * time.Second,
		prefetchCount:               5,
		checkDeadConsumerLocksOnGet: false,
		idempotencyTtl:              0,
		stopPing:                    make(chan struct{}),
	}
	_ = EnsureQueue(context.Background(), redisClient, queueName)

	if startPing {
		ctx := context.Background()
		c.ping(ctx)

		go c.pingLoop()
	}

	return c
}

// ConsumerID возвращает идентификатор консьюмера
func (c *Consumer) ConsumerID() string {
	return c.consumerID
}

// Close останавливает ping (и stats loop, если sink принадлежит этому consumer).
func (c *Consumer) Close() {
	select {
	case <-c.stopPing:
	default:
		close(c.stopPing)
	}
	if c.statsOwn && c.stats != nil {
		c.stats.Stop()
	}
}

func (c *Consumer) ensureStats() *StatsSink {
	if c.stats == nil {
		c.stats = newStatsSink(c.redis, c.queueName)
		c.statsOwn = true
	}
	return c.stats
}

// SetStatsRetention включает запись g/a/r (+ sample тегов) с TTL точек. 0 = выкл (default).
func (c *Consumer) SetStatsRetention(d time.Duration) {
	s := c.ensureStats()
	s.setRetention(d)
	if d > 0 && c.statsOwn {
		s.ensureLoop()
	}
}

// SetTagsStats — теги для секундного SCARD-сэмпла в Redis (поле t:{tag}). Пусто = только g/a/r.
// Метрики должны быть включены через SetStatsRetention.
func (c *Consumer) SetTagsStats(tags ...string) {
	c.ensureStats().setTags(tags)
}

// SetPollInterval задает интервал ожидания при отсутствии задач в очереди
func (c *Consumer) SetPollInterval(d time.Duration) {
	c.pollInterval = d
}

// SetPrefetchCount задает количество задач для предзагрузки (по умолчанию 5)
func (c *Consumer) SetPrefetchCount(n int) {
	if n < 1 {
		n = 1
	}
	c.prefetchCount = n
}

// SetCheckDeadConsumerLocksOnGet флаг проверки блокировок при get
func (c *Consumer) SetCheckDeadConsumerLocksOnGet(cd bool) {
	c.checkDeadConsumerLocksOnGet = cd
}

// SetSamePartitionBatch — Get набирает пачку из одной партиции: первая задача выбирается как обычно
// (случайная свободная группа и партиция), остальные до prefetchCount — только из её партиции.
// Готовых задач в партиции меньше — пачка меньше, без ожидания. По умолчанию выключено.
func (c *Consumer) SetSamePartitionBatch(enabled bool) {
	c.samePartitionBatch = enabled
}

// SetExtendGroupBlocksOnly — Reject с blockGroups и BlockGroups только продлевают паузу группы
// (ZADD GT, Redis >= 6.2): параллельные отказы с коротким waitTime не сокращают уже стоящую
// длинную паузу. По умолчанию выключено — каждый вызов перезаписывает unlockAt.
func (c *Consumer) SetExtendGroupBlocksOnly(enabled bool) {
	c.extendGroupBlocksOnly = enabled
}

// SetDLQMaxSize — дефолтная ёмкость DLQ для WithDeadLetter() без аргумента
// и лимит дочистки в ping (ARGV, без ключа в Redis).
// 0 (дефолт) — без лимита. >0 — вытеснение старых при Reject→DLQ (+ батчи в ping).
func (c *Consumer) SetDLQMaxSize(n int) {
	if n < 0 {
		n = 0
	}
	c.dlqMaxSize = n
}

// SetIdempotencyTtl — окно dedupe после успешного Ack: payload:{id} живёт TTL, повторный Publish с тем же ID отклоняется (NX).
// Это не rate limit: ограничивает только повтор того же ID. 0 (дефолт) — сразу удалять ключ после Ack.
func (c *Consumer) SetIdempotencyTtl(n time.Duration) {
	if n < 0 {
		n = 0
	}
	c.idempotencyTtl = n
}

// Use добавляет middleware вокруг handler в Consume.
// Порядок: первый Use — внешний слой (успевает увидеть HandlerPanicError / обычный error).
func (c *Consumer) Use(mw ...HandlerMiddleware) {
	c.middlewares = append(c.middlewares, mw...)
}

// GetChan запускает цикл чтения очереди и возвращает канал с задачами.
// Останавливается при отмене контекста или вызове Close.
func (c *Consumer) GetChan(ctx context.Context) <-chan *Task {
	ch := make(chan *Task)

	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopPing:
				return
			default:
			}

			tasks, err := c.Get(ctx)
			if err != nil {
				<-time.After(c.pollInterval)
				continue
			}

			if len(tasks) == 0 {
				<-time.After(c.pollInterval)
				continue
			}

			for _, task := range tasks {
				ch <- task
			}
		}
	}()

	return ch
}

// isOrderedPartition возвращает true для партиций с префиксом "!" (ordered queue)
func isOrderedPartition(partition string) bool {
	return len(partition) > 0 && partition[0] == '!'
}

// Consume запускает вечный цикл: Get → обработка handler → Ack при успехе, Reject при ошибке.
// Паника в handler (или middleware) перехватывается, превращается в HandlerPanicError и
// обрабатывается как обычная ошибка handler → Reject; процесс и цикл Consume не падают.
// Гарантирует последовательную обработку задач в рамках одного консьюмера.
// Для ordered-партиций (!): при reject первой задачи остальные из этой партиции в текущем
// prefetch-батче также reject'ятся без опций (plain Reject), чтобы сохранить порядок.
// При ошибке Ack или Reject консьюмер завершает работу и возвращает ошибку — остальные
// сообщения вернутся в очередь при чистке мёртвых консьюмеров, что важно для ordered-партиций:
// не брать следующую пачку тем же консьюмером и не нарушать порядок.
//
// Отмена ctx / Close останавливает цикл между задачами: текущий handler дорабатывается,
// Ack/Reject идут через context.WithoutCancel(ctx), чтобы SIGTERM не ронял подтверждение
// уже взятой задачи (иначе партиция/задача висели бы до истечения heartbeat).
// Неначатые задачи из prefetch возвращаются в очередь через Reject.
func (c *Consumer) Consume(ctx context.Context, handler func(*Task) error) error {
	// Подтверждения не должны зависеть от отмены shutdown-контекста.
	opCtx := context.WithoutCancel(ctx)
	run := applyMiddlewares(c.middlewares, handler)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.stopPing:
			return nil
		default:
		}

		tasks, err := c.Get(ctx)
		if err != nil {
			<-time.After(c.pollInterval)
			continue
		}

		if len(tasks) == 0 {
			<-time.After(c.pollInterval)
			continue
		}

		rejectedPartitions := make(map[string]bool)
		for _, task := range tasks {
			select {
			case <-ctx.Done():
				// без опций: назад в очередь (не наследуем DeadLetter/Delay с соседей)
				if err := c.Reject(opCtx, task.ID); err != nil {
					return err
				}
				continue
			case <-c.stopPing:
				if err := c.Reject(opCtx, task.ID); err != nil {
					return err
				}
				continue
			default:
			}

			if isOrderedPartition(task.Partition) && rejectedPartitions[task.Partition] {
				if err := c.Reject(opCtx, task.ID); err != nil {
					return err
				}
				continue
			}

			if err := callHandler(run, task); err != nil {
				if isOrderedPartition(task.Partition) {
					rejectedPartitions[task.Partition] = true
				}
				var rejectOpts []RejectOption
				var rejectErr *RejectWithDelay
				if errors.As(err, &rejectErr) {
					rejectOpts = rejectOptsFromDelay(rejectErr)
				}
				if ackErr := c.Reject(opCtx, task.ID, rejectOpts...); ackErr != nil {
					return ackErr
				}
			} else {
				ackOpts := make([]AckOption, 0, 3)
				if c.idempotencyTtl > 0 {
					ackOpts = append(ackOpts, WithIdempotency(c.idempotencyTtl))
				}
				if len(task.ackShared) > 0 {
					ackOpts = append(ackOpts, WithShared(task.ackShared))
				}
				if len(task.ackOpen) > 0 {
					ackOpts = append(ackOpts, WithOpenBlocks(task.ackOpen...))
				}
				if task.ackSharedTTL > 0 && (len(task.ackShared) > 0 || len(task.ackOpen) > 0) {
					ackOpts = append(ackOpts, WithSharedTTL(task.ackSharedTTL))
				}
				if ackErr := c.Ack(opCtx, task.ID, ackOpts...); ackErr != nil {
					return ackErr
				}
			}
		}
	}
}

// pingLoop обновляет heartbeat консьюмера каждую минуту
func (c *Consumer) pingLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	ctx := context.Background()

	for {
		select {
		case <-c.stopPing:
			return
		case <-ticker.C:
			c.ping(ctx)
		}
	}
}

// ping обновляет heartbeat консьюмера через Lua скрипт
func (c *Consumer) ping(ctx context.Context) error {
	_, err := getPingScript().Run(ctx, c.redis, []string{}, c.queueName, c.consumerID, c.dlqMaxSize).Result()
	return err
}

// SetJournalAck вызывается после успешного SRQ Ack (ретраи — на стороне колбэка).
func (c *Consumer) SetJournalAck(fn func(ctx context.Context, taskID string) error) {
	c.journalAck = fn
}

// SetAfterAck вызывается после успешного SRQ Ack (и после journalAck, если задан).
func (c *Consumer) SetAfterAck(fn func(ctx context.Context, taskID string) error) {
	c.afterAck = fn
}

// SetEmergencyResume — ждать srq:resume_after после ошибки Redis, похожей на потерю состояния.
func (c *Consumer) SetEmergencyResume(on bool) {
	c.emergencyResume = on
}

// SetJournalAck — как у Consumer; копируется на воркеры пула при respawn.
func (p *ConsumerPool) SetJournalAck(fn func(ctx context.Context, taskID string) error) {
	p.journalAck = fn
}

// SetAfterAck — как у Consumer; копируется на воркеры пула при respawn.
func (p *ConsumerPool) SetAfterAck(fn func(ctx context.Context, taskID string) error) {
	p.afterAck = fn
}

// SetEmergencyResume — как у Consumer; копируется на воркеры пула.
func (p *ConsumerPool) SetEmergencyResume(on bool) {
	p.emergencyResume = on
}

func (c *Consumer) waitEmergency(ctx context.Context) error {
	if !c.emergencyResume || c.faultAt.IsZero() {
		return nil
	}
	for {
		at, ok, err := GetResumeAfter(ctx, c.redis)
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		if ok && at.After(c.faultAt) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (c *Consumer) noteRedisFault(err error) {
	if !c.emergencyResume || err == nil {
		return
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "loading") || strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "eof") || strings.Contains(msg, "broken pipe") {
		c.faultAt = time.Now()
	}
}

// Get получает до prefetchCount задач из очереди и возвращает их разом
func (c *Consumer) Get(ctx context.Context) ([]*Task, error) {
	if err := c.waitEmergency(ctx); err != nil {
		return nil, err
	}
	script := getGetScript()

	args := []interface{}{
		c.queueName,
		c.consumerID,
		c.prefetchCount,
		bool01(c.checkDeadConsumerLocksOnGet),
	}
	args = appendGetRateLimitArgs(args, c.rateLimit)
	args = append(args, bool01(c.concurrencyTags), bool01(c.samePartitionBatch))

	result, err := script.Run(ctx, c.redis, []string{}, args...).Result()

	if err != nil {
		c.noteRedisFault(err)
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	results := result.([]interface{})
	if len(results) == 0 {
		return nil, nil
	}

	// Результат: {taskId, partition, payload, rejectCount, scoreMs, sharedPairs, partPrio, queuePrio, ...}
	// sharedPairs — плоский массив [id, blob, id, blob, ...] или пустой.
	// Tags/Groups на Get не отдаём (лишние SMEMBERS); смотри InspectTask.
	tasks := make([]*Task, 0, len(results)/8)
	for i := 0; i+7 < len(results); i += 8 {
		if results[i] == nil {
			continue
		}
		taskID := results[i].(string)
		partition := ""
		if results[i+1] != nil {
			partition = results[i+1].(string)
		}
		payload := ""
		if results[i+2] != nil {
			payload = results[i+2].(string)
		}
		rejectCount := 0
		if results[i+3] != nil {
			if n, err := strconv.Atoi(results[i+3].(string)); err == nil {
				rejectCount = n
			}
		}
		scheduled := time.Now()
		if results[i+4] != nil {
			if ms, err := strconv.ParseInt(results[i+4].(string), 10, 64); err == nil && ms > 0 {
				scheduled = time.UnixMilli(ms)
			}
		}
		shared := parseSharedPairs(results[i+5])
		partPrio := 0
		if results[i+6] != nil {
			if n, err := strconv.Atoi(fmt.Sprint(results[i+6])); err == nil {
				partPrio = n
			}
		}
		queuePrio := 0
		if results[i+7] != nil {
			if n, err := strconv.Atoi(fmt.Sprint(results[i+7])); err == nil {
				queuePrio = n
			}
		}

		raw := []byte(payload)
		decoded, err := decodePayloadFromRedis(raw)
		if err != nil {
			return nil, fmt.Errorf("decode payload %s: %w", taskID, err)
		}
		tasks = append(tasks, &Task{
			ID:                  taskID,
			Partition:           partition,
			PartitionPriority:   partPrio,
			QueuePriority:       queuePrio,
			Payload:             decoded,
			Scheduled:           scheduled,
			RejectCount:         rejectCount,
			Shared:              shared,
			bindRedis:           c.redis,
			bindQueue:           c.queueName,
			bindConsumerID:      c.consumerID,
			bindConcurrencyTags: c.concurrencyTags,
		})
	}

	if c.stats != nil {
		c.stats.addGet(int64(len(tasks)))
	}
	return tasks, nil
}

// Ack подтверждает обработку задачи.
// Опции: WithIdempotency, WithShared, WithSharedTTL, WithOpenBlocks.
func (c *Consumer) Ack(ctx context.Context, taskID string, opts ...AckOption) error {
	cfg := applyAckOptions(opts)
	args := []interface{}{
		c.queueName,
		taskID,
		c.consumerID,
		cfg.idempotencyTTL.Seconds(),
		bool01(c.concurrencyTags),
		sharedTTLSeconds(cfg.sharedTTL),
	}
	args = appendSharedArgs(args, cfg.shared)
	args = appendOpenBlockArgs(args, cfg.openBlocks)

	result, err := getAckScript().Run(ctx, c.redis, []string{}, args...).Result()
	if err != nil {
		return fmt.Errorf("failed to ack task: %w", err)
	}

	if result.(int64) == 0 {
		return fmt.Errorf("task not found in processing or not owned by this consumer")
	}

	if c.stats != nil {
		c.stats.addAck(1)
	}
	if c.journalAck != nil {
		if err := c.journalAck(ctx, taskID); err != nil {
			return fmt.Errorf("journal ack: %w", err)
		}
	}
	if c.afterAck != nil {
		if err := c.afterAck(ctx, taskID); err != nil {
			return fmt.Errorf("after ack: %w", err)
		}
	}
	return nil
}

// appendSharedArgs: n, id1, blob1, ...
func appendSharedArgs(args []interface{}, shared map[string][]byte) []interface{} {
	if len(shared) == 0 {
		return append(args, 0)
	}
	args = append(args, len(shared))
	for id, blob := range shared {
		args = append(args, id, string(blob))
	}
	return args
}

func parseSharedPairs(v interface{}) map[string][]byte {
	arr, ok := v.([]interface{})
	if !ok || len(arr) < 2 {
		return nil
	}
	out := make(map[string][]byte, len(arr)/2)
	for i := 0; i+1 < len(arr); i += 2 {
		if arr[i] == nil || arr[i+1] == nil {
			continue
		}
		id, okID := arr[i].(string)
		blob, okBlob := arr[i+1].(string)
		if !okID || !okBlob || id == "" {
			continue
		}
		out[id] = []byte(blob)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func rejectOptsFromDelay(e *RejectWithDelay) []RejectOption {
	if e == nil {
		return nil
	}
	opts := make([]RejectOption, 0, 3)
	if e.Delay > 0 {
		opts = append(opts, WithDelay(e.Delay))
	}
	if len(e.BlockGroups) > 0 {
		opts = append(opts, WithBlockGroups(e.BlockGroups...))
	}
	if len(e.BlockTags) > 0 {
		opts = append(opts, WithBlockTags(e.BlockTags...))
	}
	if e.DeadLetter {
		if e.DeadLetterMax < 0 {
			opts = append(opts, WithDeadLetter())
		} else {
			opts = append(opts, WithDeadLetter(e.DeadLetterMax))
		}
	}
	return opts
}

// Reject отклоняет задачу: либо назад в очередь, либо в DLQ (WithDeadLetter).
// Опции (порядок любой): WithDelay, WithBlockGroups, WithBlockTags, WithDeadLetter.
// При Delay > 0 для ordered-партиций (!) пишется partition в blocked (и при DLQ).
func (c *Consumer) Reject(ctx context.Context, taskID string, opts ...RejectOption) error {
	cfg := applyRejectOptions(opts)
	groupsStr, err := encodeGroups(cfg.blockGroups)
	if err != nil {
		return err
	}
	blockTagsStr, err := encodeBlockTagIDs(cfg.blockTags)
	if err != nil {
		return err
	}

	dlqMax := 0
	if cfg.deadLetter {
		dlqMax = cfg.dlqMax
		if dlqMax < 0 {
			dlqMax = c.dlqMaxSize
		}
	}

	result, err := getRejectScript().Run(ctx, c.redis, []string{},
		c.queueName,
		taskID,
		c.consumerID,
		cfg.delay,
		groupsStr,
		bool01(c.concurrencyTags),
		bool01(c.extendGroupBlocksOnly),
		blockTagsStr,
		bool01(cfg.deadLetter),
		dlqMax,
	).Result()

	if err != nil {
		return fmt.Errorf("failed to reject task: %w", err)
	}

	if result.(int64) == 0 {
		return fmt.Errorf("task not found in processing or not owned by this consumer")
	}

	if c.stats != nil {
		c.stats.addReject(1)
	}
	return nil
}

func encodeBlockTagIDs(ids []string) (string, error) {
	if len(ids) == 0 {
		return "", nil
	}
	parts := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, raw := range ids {
		id := NormalizeRaceBlockID(raw)
		if id == "" {
			continue
		}
		if strings.Contains(id, TagSeparator) {
			return "", fmt.Errorf("block tag id %q contains forbidden separator %q", id, TagSeparator)
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		parts = append(parts, id)
	}
	return strings.Join(parts, TagSeparator), nil
}

func appendOpenBlockArgs(args []interface{}, ids []string) []interface{} {
	if len(ids) == 0 {
		return append(args, 0)
	}
	norm := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, raw := range ids {
		id := NormalizeRaceBlockID(raw)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		norm = append(norm, id)
	}
	args = append(args, len(norm))
	for _, id := range norm {
		args = append(args, id)
	}
	return args
}

// BlockGroups паркует группы на waitTime секунд (без задачи): ZADD group → unlockAt в groups.
// Get не обходит ready этих групп, пока now < unlockAt.
func (c *Consumer) BlockGroups(ctx context.Context, waitTime float64, groups ...string) error {
	return blockGroups(ctx, c.redis, c.queueName, waitTime, c.extendGroupBlocksOnly, groups...)
}

func blockGroups(ctx context.Context, rdb *redis.Client, queueName string, waitTime float64, extendOnly bool, groups ...string) error {
	if waitTime <= 0 || len(groups) == 0 {
		return nil
	}
	groupsStr, err := encodeGroups(groups)
	if err != nil {
		return err
	}
	_, err = getBlockGroupsScript().Run(ctx, rdb, []string{}, queueName, waitTime, groupsStr, bool01(extendOnly)).Result()
	if err != nil {
		return fmt.Errorf("failed to block groups: %w", err)
	}
	return nil
}

// ConsumerPool пул консьюмеров, обрабатывающих очередь параллельно
type ConsumerPool struct {
	redis                       *redis.Client
	queueName                   string
	count                       int
	pollInterval                time.Duration
	prefetchCount               int
	checkDeadConsumerLocksOnGet bool
	idempotencyTtl              time.Duration
	rateLimit                   *RateLimitConfig
	concurrencyTags             bool
	samePartitionBatch          bool
	extendGroupBlocksOnly       bool
	dlqMaxSize                  int
	middlewares                 []HandlerMiddleware
	stats                       *StatsSink
	router                      *ShardRouter
	ownRouter                   bool
	shardStats                  []*StatsSink
	statsRetention              time.Duration
	statsTags                   []string
	journalAck                  func(ctx context.Context, taskID string) error
	afterAck                    func(ctx context.Context, taskID string) error
	emergencyResume             bool
}

func (p *ConsumerPool) ensureStats() *StatsSink {
	if p.stats == nil {
		p.stats = newStatsSink(p.redis, p.queueName)
	}
	return p.stats
}

// SetStatsRetention — см. Consumer.SetStatsRetention. Loop стартует в Consume.
func (p *ConsumerPool) SetStatsRetention(d time.Duration) {
	p.statsRetention = d
	if len(p.shardStats) > 0 {
		for _, s := range p.shardStats {
			s.setRetention(d)
		}
		return
	}
	p.ensureStats().setRetention(d)
}

// SetTagsStats — см. Consumer.SetTagsStats.
func (p *ConsumerPool) SetTagsStats(tags ...string) {
	p.statsTags = append([]string(nil), tags...)
	if len(p.shardStats) > 0 {
		for _, s := range p.shardStats {
			s.setTags(p.statsTags)
		}
		return
	}
	p.ensureStats().setTags(tags)
}

// NewConsumerPool создает пул консьюмеров. Для нескольких Redis — SetShards.
func NewConsumerPool(redisClient *redis.Client, queueName string) *ConsumerPool {
	p := &ConsumerPool{
		redis:                       redisClient,
		queueName:                   queueName,
		count:                       5,
		pollInterval:                1 * time.Second,
		prefetchCount:               5,
		checkDeadConsumerLocksOnGet: false,
		idempotencyTtl:              0,
	}
	_ = EnsureQueue(context.Background(), redisClient, queueName)
	return p
}

// SetCount задает количество консьюмеров в пуле.
// При SetShards — суммарное число; воркеры делятся по шардам.
func (p *ConsumerPool) SetCount(n int) {
	if n < 1 {
		n = 1
	}
	p.count = n
}

// SetPollInterval задает интервал ожидания при отсутствии задач
func (p *ConsumerPool) SetPollInterval(d time.Duration) {
	p.pollInterval = d
}

// SetPrefetchCount задает количество задач для предзагрузки
func (p *ConsumerPool) SetPrefetchCount(n int) {
	if n < 1 {
		n = 1
	}
	p.prefetchCount = n
}

// SetCheckDeadConsumerLocksOnGet флаг проверки блокировок при get
func (p *ConsumerPool) SetCheckDeadConsumerLocksOnGet(cd bool) {
	p.checkDeadConsumerLocksOnGet = cd
}

func (p *ConsumerPool) SetIdempotencyTtl(n time.Duration) {
	if n < 0 {
		n = 0
	}
	p.idempotencyTtl = n
}

// SetSamePartitionBatch — см. Consumer.SetSamePartitionBatch.
func (p *ConsumerPool) SetSamePartitionBatch(enabled bool) {
	p.samePartitionBatch = enabled
}

// SetExtendGroupBlocksOnly — см. Consumer.SetExtendGroupBlocksOnly.
func (p *ConsumerPool) SetExtendGroupBlocksOnly(enabled bool) {
	p.extendGroupBlocksOnly = enabled
}

// SetDLQMaxSize — см. Consumer.SetDLQMaxSize.
func (p *ConsumerPool) SetDLQMaxSize(n int) {
	if n < 0 {
		n = 0
	}
	p.dlqMaxSize = n
}

// Use добавляет middleware вокруг handler (см. Consumer.Use).
func (p *ConsumerPool) Use(mw ...HandlerMiddleware) {
	p.middlewares = append(p.middlewares, mw...)
}

// Consume запускает count консьюмеров, блокируется до отмены контекста.
// При отмене контекста все консьюмеры останавливаются, метод возвращает управление.
// Если консьюмер завершается из-за ошибки Ack/Reject, запускается новый на его место —
// число активных консьюмеров остаётся постоянным.
// Паника в handler перехватывается (см. Consumer.Consume) — падает одна задача в Reject,
// остальные консьюмеры пула продолжают работу.
// При SetShards воркеры делятся по шардам (включая ReadOnly).
func (p *ConsumerPool) Consume(ctx context.Context, handler func(*Task) error) {
	if p.router != nil {
		p.consumeSharded(ctx, handler)
		return
	}

	if p.stats != nil && p.stats.enabled() {
		p.stats.ensureLoop()
		defer p.stats.Stop()
	}

	var wg sync.WaitGroup

	for i := 0; i < p.count; i++ {
		wg.Add(1)
		go p.runConsumer(ctx, &wg, handler, p.redis, p.stats)
	}

	wg.Wait()
}

// runConsumer запускает консьюмера; при ошибке Ack/Reject перезапускает нового (respawn),
// пока контекст не отменён.
func (p *ConsumerPool) runConsumer(ctx context.Context, wg *sync.WaitGroup, handler func(*Task) error, rdb *redis.Client, stats *StatsSink) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		c := newConsumer(rdb, p.queueName, "", true)
		c.pollInterval = p.pollInterval
		c.prefetchCount = p.prefetchCount
		c.checkDeadConsumerLocksOnGet = p.checkDeadConsumerLocksOnGet
		c.idempotencyTtl = p.idempotencyTtl
		c.rateLimit = p.rateLimit
		c.concurrencyTags = p.concurrencyTags
		c.samePartitionBatch = p.samePartitionBatch
		c.extendGroupBlocksOnly = p.extendGroupBlocksOnly
		c.dlqMaxSize = p.dlqMaxSize
		c.middlewares = p.middlewares
		c.stats = stats
		c.statsOwn = false
		c.journalAck = p.journalAck
		c.afterAck = p.afterAck
		c.emergencyResume = p.emergencyResume

		err := c.Consume(ctx, handler)
		c.Close()

		if ctx.Err() != nil {
			return
		}
		// Консьюмер умер из-за ошибки Ack/Reject — пауза и перезапуск на его месте.
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(p.pollInterval):
			}
		}
	}
}
