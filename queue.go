package redisqueue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RejectWithDelay — ошибка, при которой задача возвращается в очередь с задержкой.
// Delay задаётся в секундах; для ordered-партиций (!) при waitTime > 0 ставится TTL-блок.
// Используется для ratelimit: партиция не берётся до истечения задержки.
type RejectWithDelay struct {
	Err   error
	Delay int // секунды
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

// NewRejectWithDelay создаёт ошибку с задержкой для Reject.
func NewRejectWithDelay(err error, delaySeconds int) *RejectWithDelay {
	return &RejectWithDelay{Err: err, Delay: delaySeconds}
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
	ID          string    `json:"id"`
	Partition   string    `json:"partition,omitempty"`
	Priority    int       `json:"priority,omitempty"`
	Payload     []byte    `json:"-"`
	Scheduled   time.Time `json:"scheduled"`
	CreatedAt   time.Time `json:"createdAt"`
	RejectCount int       `json:"rejectCount,omitempty"` // кол-во reject для расчёта задержки
}

// Producer отправляет задачи в очередь
type Producer struct {
	redis     *redis.Client
	queueName string
}

// NewProducer создает нового продюсера
func NewProducer(redisClient *redis.Client, queueName string) *Producer {
	return &Producer{
		redis:     redisClient,
		queueName: queueName,
	}
}

// Publish добавляет одну или несколько задач в очередь атомарно, сохраняя порядок
func (p *Producer) Publish(ctx context.Context, tasks ...*Task) error {
	if len(tasks) == 0 {
		return nil
	}

	for _, task := range tasks {
		if task.ID == "" {
			return fmt.Errorf("task ID is required")
		}
	}

	args := make([]interface{}, 0, 2+len(tasks)*4)
	args = append(args, p.queueName, len(tasks))

	for _, task := range tasks {
		scheduled := task.Scheduled.UnixMilli()
		if scheduled == 0 {
			scheduled = time.Now().UnixMilli()
		}
		args = append(args, task.ID, task.Partition, task.Priority, scheduled, string(task.Payload))
	}

	script := getAddScript()
	result, err := script.Run(ctx, p.redis, []string{}, args...).Result()
	if err != nil {
		return fmt.Errorf("failed to publish tasks: %w", err)
	}

	added := result.(int64)
	if added < int64(len(tasks)) {
		return fmt.Errorf("%d of %d tasks already exist", int64(len(tasks))-added, len(tasks))
	}

	return nil
}

// Consumer получает и обрабатывает задачи из очереди
type Consumer struct {
	redis         *redis.Client
	queueName     string
	consumerID    string
	pollInterval  time.Duration
	prefetchCount int
	stopPing      chan struct{}
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
		redis:         redisClient,
		queueName:     queueName,
		consumerID:    consumerID,
		pollInterval:  1 * time.Second,
		prefetchCount: 5,
		stopPing:      make(chan struct{}),
	}

	if startPing {
		go c.pingLoop()
	}

	return c
}

// ConsumerID возвращает идентификатор консьюмера
func (c *Consumer) ConsumerID() string {
	return c.consumerID
}

// Close останавливает ping горутину
func (c *Consumer) Close() {
	close(c.stopPing)
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
// Гарантирует последовательную обработку задач в рамках одного консьюмера.
// Для ordered-партиций (!): при reject первой задачи остальные из этой партиции в текущем
// prefetch-батче также reject'ятся, чтобы сохранить порядок при возврате в очередь.
// При ошибке Ack или Reject консьюмер завершает работу и возвращает ошибку — остальные
// сообщения вернутся в очередь при чистке мёртвых консьюмеров, что важно для ordered-партиций:
// не брать следующую пачку тем же консьюмером и не нарушать порядок.
func (c *Consumer) Consume(ctx context.Context, handler func(*Task) error) error {
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
				if err := c.Reject(ctx, task.ID, 0); err != nil {
					return err
				}
				continue
			case <-c.stopPing:
				if err := c.Reject(ctx, task.ID, 0); err != nil {
					return err
				}
				continue
			default:
			}

			if isOrderedPartition(task.Partition) && rejectedPartitions[task.Partition] {
				if err := c.Reject(ctx, task.ID, 0); err != nil {
					return err
				}
				continue
			}

			if err := handler(task); err != nil {
				if isOrderedPartition(task.Partition) {
					rejectedPartitions[task.Partition] = true
				}
				waitTime := 0
				var rejectErr *RejectWithDelay
				if errors.As(err, &rejectErr) && rejectErr.Delay > 0 {
					waitTime = rejectErr.Delay
				}
				if ackErr := c.Reject(ctx, task.ID, waitTime); ackErr != nil {
					return ackErr
				}
			} else {
				if ackErr := c.Ack(ctx, task.ID); ackErr != nil {
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
	c.ping(ctx)

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
	script := getPingScript()
	_, err := script.Run(ctx, c.redis, []string{}, c.queueName, c.consumerID).Result()
	return err
}

// Get получает до prefetchCount задач из очереди и возвращает их разом
func (c *Consumer) Get(ctx context.Context) ([]*Task, error) {
	script := getGetScript()

	result, err := script.Run(ctx, c.redis, []string{},
		c.queueName,
		c.consumerID,
		c.prefetchCount,
	).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	results := result.([]interface{})
	if len(results) == 0 {
		return nil, nil
	}

	// Результат: {taskId1, partition1, payload1, rejectCount1, taskId2, ...}
	tasks := make([]*Task, 0, len(results)/4)
	for i := 0; i+3 < len(results); i += 4 {
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

		tasks = append(tasks, &Task{
			ID:          taskID,
			Partition:   partition,
			Payload:     []byte(payload),
			Scheduled:   time.Now(),
			CreatedAt:   time.Now(),
			RejectCount: rejectCount,
		})
	}

	return tasks, nil
}

// Ack подтверждает обработку задачи
func (c *Consumer) Ack(ctx context.Context, taskID string) error {
	script := getAckScript()

	result, err := script.Run(ctx, c.redis, []string{},
		c.queueName,
		taskID,
		c.consumerID,
	).Result()

	if err != nil {
		return fmt.Errorf("failed to ack task: %w", err)
	}

	if result.(int64) == 0 {
		return fmt.Errorf("task not found in processing or not owned by this consumer")
	}

	return nil
}

// Reject отклоняет задачу и возвращает её обратно в очередь.
// waitTime — в секундах; для ordered-партиций (!) при waitTime > 0 ставится TTL-блок,
// партиция не берётся до истечения (кейс ratelimit: нет смысла гонять одно сообщение туда-обратно).
func (c *Consumer) Reject(ctx context.Context, taskID string, waitTime int) error {
	script := getRejectScript()

	result, err := script.Run(ctx, c.redis, []string{},
		c.queueName,
		taskID,
		c.consumerID,
		waitTime,
	).Result()

	if err != nil {
		return fmt.Errorf("failed to reject task: %w", err)
	}

	if result.(int64) == 0 {
		return fmt.Errorf("task not found in processing or not owned by this consumer")
	}

	return nil
}

// ConsumerPool пул консьюмеров, обрабатывающих очередь параллельно
type ConsumerPool struct {
	redis         *redis.Client
	queueName     string
	count         int
	pollInterval  time.Duration
	prefetchCount int
}

// NewConsumerPool создает пул консьюмеров
func NewConsumerPool(redisClient *redis.Client, queueName string) *ConsumerPool {
	return &ConsumerPool{
		redis:         redisClient,
		queueName:     queueName,
		count:         5,
		pollInterval:  1 * time.Second,
		prefetchCount: 5,
	}
}

// SetCount задает количество консьюмеров в пуле
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

// Consume запускает count консьюмеров, блокируется до отмены контекста.
// При отмене контекста все консьюмеры останавливаются, метод возвращает управление.
// Если консьюмер завершается из-за ошибки Ack/Reject, запускается новый на его место —
// число активных консьюмеров остаётся постоянным.
func (p *ConsumerPool) Consume(ctx context.Context, handler func(*Task) error) {
	var wg sync.WaitGroup

	for i := 0; i < p.count; i++ {
		wg.Add(1)
		go p.runConsumer(ctx, &wg, handler)
	}

	wg.Wait()
}

// runConsumer запускает консьюмера; при ошибке Ack/Reject перезапускает нового (respawn),
// пока контекст не отменён.
func (p *ConsumerPool) runConsumer(ctx context.Context, wg *sync.WaitGroup, handler func(*Task) error) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		c := newConsumer(p.redis, p.queueName, "", true)
		c.pollInterval = p.pollInterval
		c.prefetchCount = p.prefetchCount

		c.Consume(ctx, handler)
		c.Close()

		if ctx.Err() != nil {
			return
		}
		// Консьюмер умер из-за ошибки Ack/Reject — перезапускаем на его месте
	}
}
