package redisqueue

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redis/go-redis/v9"
)

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

func newClient(c RedisConfig) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     c.Addr,
		Password: c.Password,
		DB:       c.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return rdb, nil
}

func setupTestQueue(t *testing.T) (*Producer, *Consumer, *redis.Client) {
	cfg := RedisConfig{
		Addr:     "localhost:6379",
		Password: "",
		DB:       5, // Отдельная БД для тестов
	}

	client, err := newClient(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	ctx := context.Background()
	client.FlushDB(ctx)

	producer := NewProducer(client, "test-queue")
	consumer := NewConsumer(client, "test-queue", "")
	t.Cleanup(func() {
		consumer.Close()
	})
	return producer, consumer, client
}

// Вспомогательные функции для тестов
func taskKey(queueName, taskID string) string {
	return fmt.Sprintf("queue:%s:task:%s", queueName, taskID)
}

func queueKey(queueName string) string {
	return fmt.Sprintf("queue:%s:partition:base:0", queueName)
}

func partitionKey(queueName, partition, priority string) string {
	return fmt.Sprintf("queue:%s:partition:%s:%s", queueName, partition, priority)
}

func partitionLockKey(queueName, partition string) string {
	return fmt.Sprintf("queue:%s:partition:%s:lock", queueName, partition)
}

func partitionBlockKey(queueName, partition string) string {
	return fmt.Sprintf("queue:%s:partition:%s:block", queueName, partition)
}

func payloadKey(queueName, taskID string) string {
	return fmt.Sprintf("queue:%s:payload:%s", queueName, taskID)
}

func consumerKey(queueName, consumerID string) string {
	return fmt.Sprintf("queue:%s:consumer:%s", queueName, consumerID)
}

func TestQueue_Add_NonPartitioned(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Partition: "",
		Payload:   []byte("test payload"),
		Scheduled: time.Now(),
	}

	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	// Проверяем что задача добавлена
	count, err := consumer.redis.ZCard(ctx, queueKey("test-queue")).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestQueue_Add_Partitioned(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Partition: "partition-1",
		Payload:   []byte("test payload"),
		Scheduled: time.Now(),
	}

	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	// Проверяем что задача добавлена в партицию
	count, err := consumer.redis.ZCard(ctx, partitionKey("test-queue", "partition-1", "0")).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestQueue_Add_Duplicate(t *testing.T) {
	producer, _, _ := setupTestQueue(t)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Payload:   []byte("test payload"),
		Scheduled: time.Now(),
	}

	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	// Пытаемся добавить дубликат
	err = producer.Publish(ctx, task)
	assert.Error(t, err)
}

func TestQueue_Get_NonPartitioned(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Payload:   []byte("test payload"),
		Scheduled: time.Now().Add(-time.Second),
	}

	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got)
	assert.Equal(t, "task-1", got[0].ID)
	assert.Equal(t, []byte("test payload"), got[0].Payload)
}

func TestQueue_Get_Partitioned(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Partition: "!partition-1", // партиция с "!" блокируется
		Payload:   []byte("test payload"),
		Scheduled: time.Now().Add(-time.Second),
	}

	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got)
	assert.Equal(t, "task-1", got[0].ID)
	assert.Equal(t, "!partition-1", got[0].Partition)
	assert.Equal(t, []byte("test payload"), got[0].Payload)

	// Проверяем что партиция с "!" заблокирована
	lockOwner, err := consumer.redis.Get(ctx, partitionLockKey("test-queue", "!partition-1")).Result()
	require.NoError(t, err)
	assert.Equal(t, consumer.ConsumerID(), lockOwner)
}

func TestQueue_Get_PartitionOrdering(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		task := &Task{
			ID:        fmt.Sprintf("task-%d", i),
			Partition: "!partition-1", // с блокировкой — по одной задаче за Get до ack
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			Scheduled: time.Now().Add(-time.Second),
		}
		err := producer.Publish(ctx, task)
		require.NoError(t, err)
	}

	for i := 1; i <= 3; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, got)
		assert.Equal(t, fmt.Sprintf("task-%d", i), got[0].ID)
		err = consumer.Ack(ctx, got[0].ID)
		require.NoError(t, err)
	}
}

func TestQueue_Get_PrefetchMultiple(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(5)
	ctx := context.Background()

	// Публикуем 7 задач
	for i := 1; i <= 7; i++ {
		task := &Task{
			ID:        fmt.Sprintf("task-%d", i),
			Partition: "partition-" + strconv.Itoa(i),
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			Scheduled: time.Now().Add(-time.Second),
		}
		err := producer.Publish(ctx, task)
		require.NoError(t, err)
	}

	// Первый Get должен вернуть до 5 задач (prefetchCount)
	got1, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got1)
	assert.GreaterOrEqual(t, len(got1), 1)
	assert.LessOrEqual(t, len(got1), 5)
	firstBatchSize := len(got1)

	for _, task := range got1 {
		err = consumer.Ack(ctx, task.ID)
		require.NoError(t, err)
	}

	// Второй Get - оставшиеся задачи
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, 7-firstBatchSize, len(got2))
	for _, task := range got2 {
		err = consumer.Ack(ctx, task.ID)
		require.NoError(t, err)
	}

	// Очередь пуста
	got3, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got3)
}

func TestQueue_Get_PrefetchSingle(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1) // Явно 1 для обратной совместимости
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Payload:   []byte("payload"),
		Scheduled: time.Now().Add(-time.Second),
	}
	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "task-1", got[0].ID)
}

func TestQueue_Get_Scheduled(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Payload:   []byte("test payload"),
		Scheduled: time.Now().Add(2 * time.Second),
	}

	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	// Пытаемся получить - не должна быть доступна
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got)

	// Ждем и получаем
	time.Sleep(2 * time.Second)
	got, err = consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got)
	assert.Equal(t, "task-1", got[0].ID)
}

func TestQueue_Ack(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Payload:   []byte("test payload"),
		Scheduled: time.Now().Add(-time.Second),
	}

	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got)
	err = consumer.Ack(ctx, "task-1")
	require.NoError(t, err)

	// Проверяем что задача удалена
	exists, err := consumer.redis.Exists(ctx, taskKey("test-queue", "task-1")).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists)

	// Проверяем что payload удален
	exists, err = consumer.redis.Exists(ctx, payloadKey("test-queue", "task-1")).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists)
}

func TestQueue_Ack_PartitionUnlock(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Partition: "!partition-1", // партиция с "!" блокируется и разблокируется при ack
		Payload:   []byte("test payload"),
		Scheduled: time.Now().Add(-time.Second),
	}

	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got)

	// Проверяем что партиция заблокирована
	lockOwner, err := consumer.redis.Get(ctx, partitionLockKey("test-queue", "!partition-1")).Result()
	require.NoError(t, err)
	assert.Equal(t, consumer.ConsumerID(), lockOwner)

	err = consumer.Ack(ctx, "task-1")
	require.NoError(t, err)

	// Проверяем что партиция разблокирована
	exists, err := consumer.redis.Exists(ctx, partitionLockKey("test-queue", "!partition-1")).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists)
}

func TestQueue_Reject(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Payload:   []byte("test payload"),
		Scheduled: time.Now().Add(-time.Second),
	}

	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got)

	err = consumer.Reject(ctx, "task-1", 0)
	require.NoError(t, err)

	// Проверяем что задача вернулась в очередь
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got2)
	assert.Equal(t, "task-1", got2[0].ID)
}

func TestQueue_Reject_PartitionUnlock(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Partition: "!partition-1", // партиция с "!" разблокируется при reject
		Payload:   []byte("test payload"),
		Scheduled: time.Now().Add(-time.Second),
	}

	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got)

	err = consumer.Reject(ctx, "task-1", 0)
	require.NoError(t, err)

	// Проверяем что партиция разблокирована
	exists, err := consumer.redis.Exists(ctx, partitionLockKey("test-queue", "!partition-1")).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists)

	// Задача должна быть доступна снова
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got2)
	assert.Equal(t, "task-1", got2[0].ID)
}

// TestQueue_Reject_OrderedPartition_WithWaitTime проверяет, что при Reject с waitTime > 0
// для ordered-партиции ставится TTL-блок: партиция не берётся до истечения (кейс ratelimit).
func TestQueue_Reject_OrderedPartition_WithWaitTime(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Partition: "!ratelimit-partition",
		Payload:   []byte("test payload"),
		Scheduled: time.Now().Add(-time.Second),
	}
	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got)
	assert.Equal(t, "task-1", got[0].ID)

	// Reject с waitTime=2 сек — партиция блокируется на 2 секунды
	err = consumer.Reject(ctx, "task-1", 2)
	require.NoError(t, err)

	// Блок-ключ должен существовать с TTL ~2 сек
	blockKey := partitionBlockKey("test-queue", "!ratelimit-partition")
	exists, err := consumer.redis.Exists(ctx, blockKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists, "block key должен быть установлен")

	ttl, err := consumer.redis.TTL(ctx, blockKey).Result()
	require.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= 2*time.Second, "TTL должен быть около 2 сек, got %v", ttl)

	// Get не должен вернуть задачу — партиция заблокирована
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2, "партиция заблокирована — задача не берётся")

	// Ждём истечения TTL
	time.Sleep(2*time.Second + 100*time.Millisecond)

	// После истечения блок снимается — задача снова доступна
	got3, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got3)
	assert.Equal(t, "task-1", got3[0].ID)

	require.NoError(t, consumer.Ack(ctx, "task-1"))
}

// TestQueue_Consume_RejectWithDelay проверяет, что handler может вернуть RejectWithDelay
// и задержка используется при Reject (для ordered-партиции ставится TTL-блок).
func TestQueue_Consume_RejectWithDelay(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	consumer.SetPollInterval(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := &Task{
		ID:        "task-1",
		Partition: "!ratelimit",
		Payload:   []byte("rate limited"),
		Scheduled: time.Now().Add(-time.Second),
	}
	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	handled := make(chan struct{})
	go func() {
		consumer.Consume(ctx, func(t *Task) error {
			close(handled)
			return NewRejectWithDelay(fmt.Errorf("rate limit"), 2)
		})
	}()

	<-handled
	time.Sleep(100 * time.Millisecond)

	// Block key должен быть установлен с TTL ~2 сек
	blockKey := partitionBlockKey("test-queue", "!ratelimit")
	exists, err := consumer.redis.Exists(ctx, blockKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists, "block key должен быть установлен при RejectWithDelay")

	ttl, err := consumer.redis.TTL(ctx, blockKey).Result()
	require.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= 2*time.Second, "TTL должен быть около 2 сек, got %v", ttl)

	cancel()
}

// TestQueue_Reject_NonOrderedPartition_WaitTimeIgnored проверяет, что для не-ordered партиции
// waitTime игнорируется — блок не ставится.
func TestQueue_Reject_NonOrderedPartition_WaitTimeIgnored(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Partition: "shared-partition",
		Payload:   []byte("test payload"),
		Scheduled: time.Now().Add(-time.Second),
	}
	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got)

	// Reject с waitTime=10 — для не-ordered ничего не должно произойти
	err = consumer.Reject(ctx, "task-1", 10)
	require.NoError(t, err)

	// Block key не должен существовать для не-ordered партиции
	blockKey := partitionBlockKey("test-queue", "shared-partition")
	exists, err := consumer.redis.Exists(ctx, blockKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists, "block key не должен ставиться для не-ordered")

	// Задача сразу доступна снова
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got2)
	assert.Equal(t, "task-1", got2[0].ID)
}

// TestQueue_Reject_OrderedPartition_WaitTimeZero проверяет, что при waitTime=0 блок не ставится.
func TestQueue_Reject_OrderedPartition_WaitTimeZero(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Partition: "!ordered",
		Payload:   []byte("test payload"),
		Scheduled: time.Now().Add(-time.Second),
	}
	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got)

	err = consumer.Reject(ctx, "task-1", 0)
	require.NoError(t, err)

	blockKey := partitionBlockKey("test-queue", "!ordered")
	exists, err := consumer.redis.Exists(ctx, blockKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists, "при waitTime=0 block не ставится")

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got2)
	assert.Equal(t, "task-1", got2[0].ID)
}

func TestQueue_Ping(t *testing.T) {
	_, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	// Ждем немного чтобы ping успел выполниться
	time.Sleep(100 * time.Millisecond)

	// Проверяем что консьюмер зарегистрирован
	exists, err := consumer.redis.Exists(ctx, consumerKey("test-queue", consumer.ConsumerID())).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists)

	// Проверяем TTL
	ttl, err := consumer.redis.TTL(ctx, consumerKey("test-queue", consumer.ConsumerID())).Result()
	require.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= 2*time.Minute)
}

func TestQueue_MultipleConsumers(t *testing.T) {
	producer, consumer1, client := setupTestQueue(t)
	consumer2 := NewConsumer(client, "test-queue", "")
	t.Cleanup(func() {
		consumer2.Close()
	})
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		task := &Task{
			ID:        fmt.Sprintf("task-%d", i),
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			Scheduled: time.Now().Add(-time.Second),
		}
		err := producer.Publish(ctx, task)
		require.NoError(t, err)
	}

	var got1, got2 []string
	for len(got1)+len(got2) < 5 {
		tasks, err := consumer1.Get(ctx)
		require.NoError(t, err)
		for _, task := range tasks {
			got1 = append(got1, task.ID)
			consumer1.Ack(ctx, task.ID)
		}
		if len(got1)+len(got2) >= 5 {
			break
		}
		tasks2, err := consumer2.Get(ctx)
		require.NoError(t, err)
		for _, task := range tasks2 {
			got2 = append(got2, task.ID)
			consumer2.Ack(ctx, task.ID)
		}
	}

	assert.Equal(t, 5, len(got1)+len(got2), "должны потреблять все 5 задач")
	assert.True(t, len(got1) > 0 || len(got2) > 0)
}

func TestQueue_DeadConsumerUnlock(t *testing.T) {
	cfg := RedisConfig{Addr: "localhost:6379", Password: "", DB: 5}
	client, err := newClient(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	ctx := context.Background()
	client.FlushDB(ctx)

	queueName := fmt.Sprintf("test-queue-dead-%d", time.Now().UnixNano())
	producer := NewProducer(client, queueName)
	consumer1 := NewConsumer(client, queueName, "")
	consumer1.SetPrefetchCount(1)

	task := &Task{
		ID:        "task-1",
		Partition: "!partition-1", // партиция с "!" - ping разблокирует при мёртвом консьюмере
		Payload:   []byte("test payload"),
		Scheduled: time.Now().Add(-time.Second),
	}

	err = producer.Publish(ctx, task)
	require.NoError(t, err)

	got1, err := consumer1.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got1)

	// Проверяем что партиция заблокирована
	lockOwner, err := consumer1.redis.Get(ctx, partitionLockKey(queueName, "!partition-1")).Result()
	require.NoError(t, err)
	assert.Equal(t, consumer1.ConsumerID(), lockOwner)

	// Останавливаем pingLoop и симулируем смерть — удаляем ключ heartbeat
	consumer1.Close()
	err = client.Del(ctx, consumerKey(queueName, consumer1.ConsumerID())).Err()
	require.NoError(t, err)

	// consumer2 без pingLoop — наш ping() гарантированно обработает мёртвого
	consumer2 := newConsumer(client, queueName, "", false)
	t.Cleanup(func() { consumer2.Close() })
	consumer2.SetPrefetchCount(1)
	err = consumer2.ping(ctx)
	require.NoError(t, err)

	// Второй консьюмер должен получить задачу
	got2, err := consumer2.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got2)
	assert.Equal(t, "task-1", got2[0].ID)
}

func TestQueue_Publish_MultipleTasks(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	tasks := []*Task{
		{ID: "task-1", Payload: []byte("payload-1"), Scheduled: time.Now().Add(-time.Second)},
		{ID: "task-2", Partition: "p1", Payload: []byte("payload-2"), Scheduled: time.Now().Add(-time.Second)},
		{ID: "task-3", Payload: []byte("payload-3"), Scheduled: time.Now().Add(-time.Second)},
	}

	err := producer.Publish(ctx, tasks...)
	require.NoError(t, err)

	// Проверяем обычную очередь (task-1, task-3)
	count, err := consumer.redis.ZCard(ctx, queueKey("test-queue")).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// Проверяем партиционированную очередь (task-2)
	count, err = consumer.redis.ZCard(ctx, partitionKey("test-queue", "p1", "0")).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Получаем задачи и проверяем порядок и содержимое
	var received []*Task
	for i := 0; i < 3; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		if len(got) == 0 {
			break
		}
		for _, task := range got {
			received = append(received, task)
			err = consumer.Ack(ctx, task.ID)
			require.NoError(t, err)
		}
	}

	assert.Equal(t, 3, len(received))

	ids := make([]string, len(received))
	for i, r := range received {
		ids[i] = r.ID
	}
	assert.Contains(t, ids, "task-1")
	assert.Contains(t, ids, "task-2")
	assert.Contains(t, ids, "task-3")
}

func TestQueue_Publish_MultipleTasks_WithDuplicate(t *testing.T) {
	producer, _, _ := setupTestQueue(t)
	ctx := context.Background()

	// Сначала добавляем одну задачу
	err := producer.Publish(ctx, &Task{ID: "task-1", Payload: []byte("original"), Scheduled: time.Now()})
	require.NoError(t, err)

	// Пытаемся добавить батч с дубликатом
	err = producer.Publish(ctx, &Task{ID: "task-1", Payload: []byte("dup"), Scheduled: time.Now()}, &Task{ID: "task-2", Payload: []byte("new"), Scheduled: time.Now()})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 2 tasks already exist")
}

func TestQueue_Consume(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPollInterval(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := consumer.GetChan(ctx)

	// Публикуем 3 задачи
	err := producer.Publish(ctx,
		&Task{ID: "task-1", Payload: []byte("p1"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "task-2", Payload: []byte("p2"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "task-3", Payload: []byte("p3"), Scheduled: time.Now().Add(-time.Second)},
	)
	require.NoError(t, err)

	// Читаем все 3 задачи из канала
	var received []string
	for i := 0; i < 3; i++ {
		select {
		case task := <-ch:
			require.NotNil(t, task)
			received = append(received, task.ID)
			err := consumer.Ack(ctx, task.ID)
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for task from Consume channel")
		}
	}

	assert.Len(t, received, 3)
	assert.Contains(t, received, "task-1")
	assert.Contains(t, received, "task-2")
	assert.Contains(t, received, "task-3")

	// Канал не должен отдавать ничего, пока очередь пуста
	select {
	case task := <-ch:
		t.Fatalf("expected no task, got %v", task)
	case <-time.After(150 * time.Millisecond):
	}

	// Отмена контекста закрывает канал
	cancel()
	_, ok := <-ch
	assert.False(t, ok, "channel should be closed after context cancel")
}

func TestQueue_ConsumeWithHandler(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPollInterval(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var received []string
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		consumer.Consume(ctx, func(task *Task) error {
			mu.Lock()
			received = append(received, task.ID)
			n := len(received)
			mu.Unlock()
			if n >= 3 {
				close(done)
			}
			return nil // Ack
		})
	}()

	err := producer.Publish(ctx,
		&Task{ID: "task-1", Payload: []byte("p1"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "task-2", Payload: []byte("p2"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "task-3", Payload: []byte("p3"), Scheduled: time.Now().Add(-time.Second)},
	)
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for Consume handler")
	}

	cancel()
	time.Sleep(100 * time.Millisecond) // даём Consume выйти

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, received, 3)
	assert.Equal(t, []string{"task-1", "task-2", "task-3"}, received, "порядок сообщений")
}

func setupTestConsumerPool(t *testing.T) (*Producer, *ConsumerPool, *redis.Client) {
	cfg := RedisConfig{
		Addr:     "localhost:6379",
		Password: "",
		DB:       5,
	}
	client, err := newClient(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	ctx := context.Background()
	client.FlushDB(ctx)
	producer := NewProducer(client, "test-pool-queue")
	pool := NewConsumerPool(client, "test-pool-queue")
	return producer, pool, client
}

func TestQueue_ConsumerPool_Consume(t *testing.T) {
	producer, pool, _ := setupTestConsumerPool(t)
	pool.SetCount(3)
	pool.SetPollInterval(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Публикуем 10 задач
	for i := 1; i <= 10; i++ {
		err := producer.Publish(ctx, &Task{
			ID:        fmt.Sprintf("task-%d", i),
			Payload:   []byte(fmt.Sprintf("p%d", i)),
			Scheduled: time.Now().Add(-time.Second),
		})
		require.NoError(t, err)
	}

	var received []string
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		pool.Consume(ctx, func(task *Task) error {
			mu.Lock()
			received = append(received, task.ID)
			n := len(received)
			mu.Unlock()
			if n >= 10 {
				close(done)
			}
			return nil
		})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for all tasks")
	}

	cancel()
	time.Sleep(200 * time.Millisecond) // даём Consume выйти

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, received, 10)
	expected := []string{"task-1", "task-2", "task-3", "task-4", "task-5", "task-6", "task-7", "task-8", "task-9", "task-10"}
	assert.ElementsMatch(t, expected, received)
}

func TestQueue_ConsumerPool_ContextCancelStopsAll(t *testing.T) {
	producer, pool, _ := setupTestConsumerPool(t)
	pool.SetCount(5)
	pool.SetPollInterval(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())

	// Публикуем много задач, но отменим контекст до обработки всех
	for i := 1; i <= 100; i++ {
		_ = producer.Publish(ctx, &Task{
			ID:        fmt.Sprintf("task-%d", i),
			Payload:   []byte("x"),
			Scheduled: time.Now().Add(-time.Second),
		})
	}

	done := make(chan struct{})
	go func() {
		pool.Consume(ctx, func(task *Task) error {
			return nil
		})
		close(done)
	}()

	// Ждём немного, чтобы консьюмеры начали работать
	time.Sleep(200 * time.Millisecond)

	// Отменяем контекст — Consume должен выйти
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Consume did not exit after context cancel")
	}
}

func TestQueue_ConsumeWithHandler_Reject(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	consumer.SetPollInterval(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := producer.Publish(ctx,
		&Task{ID: "task-reject", Payload: []byte("reject-me"), Scheduled: time.Now().Add(-time.Second)},
	)
	require.NoError(t, err)

	processed := 0
	go func() {
		consumer.Consume(ctx, func(task *Task) error {
			processed++
			if task.ID == "task-reject" {
				return fmt.Errorf("intentional reject")
			}
			return nil
		})
	}()

	time.Sleep(500 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)

	// Задача отклонена и вернулась в очередь, консьюмер выходил по ctx
	assert.GreaterOrEqual(t, processed, 1)
}

// TestQueue_Reject_OrderedPartition_PreservesOrder проверяет, что при reject задач из
// ordered-партиции с prefetch порядок сохраняется: 1,2,3,4,5 → Get(2) даёт [1,2] →
// reject 1 и 2 → при следующих Get получаем 1,2,3,4,5.
func TestQueue_Reject_OrderedPartition_PreservesOrder(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(2)
	ctx := context.Background()

	partition := "!ordered"
	for i := 1; i <= 5; i++ {
		err := producer.Publish(ctx, &Task{
			ID:        fmt.Sprintf("task-%d", i),
			Partition: partition,
			Payload:   []byte(fmt.Sprintf("p%d", i)),
			Scheduled: time.Now().Add(-time.Second),
		})
		require.NoError(t, err)
	}

	// Get берёт 2 задачи: 1 и 2
	got1, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got1, 2)
	assert.Equal(t, "task-1", got1[0].ID)
	assert.Equal(t, "task-2", got1[1].ID)

	// Reject обе — для ordered партиции при reject первой все из батча идут обратно
	// (в Consume это делает rejectedPartitions; здесь вызываем Reject вручную для обеих)
	err = consumer.Reject(ctx, "task-1", 0)
	require.NoError(t, err)
	err = consumer.Reject(ctx, "task-2", 0)
	require.NoError(t, err)

	// Следующие Get должны вернуть 1, 2, 3, 4, 5 по порядку (reject с priority+1 обрабатываются первыми)
	var order []string
	for len(order) < 5 {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		if len(got) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for _, task := range got {
			order = append(order, task.ID)
			require.NoError(t, consumer.Ack(ctx, task.ID))
		}
	}

	assert.Equal(t, []string{"task-1", "task-2", "task-3", "task-4", "task-5"}, order,
		"порядок должен сохраниться после reject")
}

// TestQueue_Consume_OrderedPartition_RejectCascade проверяет, что при reject первой задачи
// из ordered-партиции в prefetch-батче остальные из той же партиции тоже reject'ятся
// и возвращаются в правильном порядке.
func TestQueue_Consume_OrderedPartition_RejectCascade(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(3)
	consumer.SetPollInterval(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	partition := "!ordered"
	for i := 1; i <= 5; i++ {
		err := producer.Publish(ctx, &Task{
			ID:        fmt.Sprintf("task-%d", i),
			Partition: partition,
			Payload:   []byte(fmt.Sprintf("p%d", i)),
			Scheduled: time.Now().Add(-time.Second),
		})
		require.NoError(t, err)
	}

	var received []string
	var mu sync.Mutex
	done := make(chan struct{})
	task1FailedOnce := false

	go func() {
		consumer.Consume(ctx, func(task *Task) error {
			mu.Lock()
			received = append(received, task.ID)
			shouldFail := task.ID == "task-1" && !task1FailedOnce
			if shouldFail {
				task1FailedOnce = true
			}
			// Ждём все 5 уникальных: 1,2,3,4,5 (task-1 может быть дважды из-за retry)
			seen := make(map[string]bool)
			for _, id := range received {
				seen[id] = true
			}
			allDone := len(seen) >= 5
			mu.Unlock()
			if allDone {
				close(done)
			}
			if shouldFail {
				return fmt.Errorf("fail first time")
			}
			return nil
		})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for all tasks")
	}

	cancel()
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	// received включает и failed task-1, затем acked 1,2,3,4,5 — порядок первых вхождений = 1,2,3,4,5
	seen := make(map[string]bool)
	var order []string
	for _, id := range received {
		if !seen[id] {
			seen[id] = true
			order = append(order, id)
		}
	}
	assert.Equal(t, []string{"task-1", "task-2", "task-3", "task-4", "task-5"}, order,
		"порядок сохраняется: reject task-1 → cascade 2,3 → затем 1,2,3,4,5")
}

// TestQueue_Reject_OrderedPartition_RepeatedRejects проверяет, что при повторных reject
// одной задачи порядок не нарушается (priority увеличивается: 0→1→2…).
func TestQueue_Reject_OrderedPartition_RepeatedRejects(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	err := producer.Publish(ctx, &Task{
		ID:        "task-1",
		Partition: "!ordered",
		Payload:   []byte("p1"),
		Scheduled: time.Now().Add(-time.Second),
	})
	require.NoError(t, err)

	// Reject 3 раза — задача должна каждый раз возвращаться
	for i := 0; i < 3; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "task-1", got[0].ID)
		err = consumer.Reject(ctx, "task-1", 0)
		require.NoError(t, err)
	}

	// Финальный Get + Ack
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "task-1", got[0].ID)
	require.NoError(t, consumer.Ack(ctx, "task-1"))

	// Очередь пуста
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)
}

func TestQueue_PartitionStrictOrdering(t *testing.T) {
	producer, consumer1, client := setupTestQueue(t)
	consumer2 := NewConsumer(client, "test-queue", "")
	t.Cleanup(func() {
		consumer2.Close()
	})
	consumer1.SetPrefetchCount(1)
	consumer2.SetPrefetchCount(1)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		task := &Task{
			ID:        fmt.Sprintf("task-%d", i),
			Partition: "!partition-1", // партиция с "!" - эксклюзивна для одного консьюмера
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			Scheduled: time.Now().Add(-time.Second),
		}
		err := producer.Publish(ctx, task)
		require.NoError(t, err)
	}

	got1, err := consumer1.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got1)
	assert.Equal(t, "task-1", got1[0].ID)

	// Второй консьюмер не может получить задачу из заблокированной партиции
	got2, err := consumer2.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)

	err = consumer1.Ack(ctx, "task-1")
	require.NoError(t, err)

	// Теперь второй консьюмер может получить следующую задачу
	got2, err = consumer2.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got2)
	assert.Equal(t, "task-2", got2[0].ID)
}

// TestQueue_PartitionWithLock_ExclusiveToWorker проверяет, что партиции с префиксом "!"
// недоступны параллельным воркерам — только один консьюмер может обрабатывать такую партицию.
func TestQueue_PartitionWithLock_ExclusiveToWorker(t *testing.T) {
	producer, consumer1, client := setupTestQueue(t)
	consumer2 := NewConsumer(client, "test-queue", "")
	t.Cleanup(func() {
		consumer2.Close()
	})
	consumer1.SetPrefetchCount(1)
	consumer2.SetPrefetchCount(1)
	ctx := context.Background()

	for i := 1; i <= 2; i++ {
		task := &Task{
			ID:        fmt.Sprintf("task-%d", i),
			Partition: "!exclusive",
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			Scheduled: time.Now().Add(-time.Second),
		}
		err := producer.Publish(ctx, task)
		require.NoError(t, err)
	}

	// consumer1 получает первую задачу и держит блокировку
	got1, err := consumer1.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got1, 1)
	assert.Equal(t, "task-1", got1[0].ID)

	// consumer2 не может получить задачу из этой партиции — она заблокирована
	got2, err := consumer2.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2, "партиция с ! эксклюзивна — второй воркер не должен получить задачу")

	// Проверяем наличие блокировки
	lockOwner, err := consumer1.redis.Get(ctx, partitionLockKey("test-queue", "!exclusive")).Result()
	require.NoError(t, err)
	assert.Equal(t, consumer1.ConsumerID(), lockOwner)

	consumer1.Ack(ctx, "task-1")

	// После ack блокировка снята — consumer2 получает task-2
	got2, err = consumer2.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	assert.Equal(t, "task-2", got2[0].ID)
	consumer2.Ack(ctx, "task-2")
}

// TestQueue_PartitionWithoutLock_SharedByWorkers проверяет, что партиции без префикса "!"
// доступны параллельным воркерам — блокировка не применяется.
func TestQueue_PartitionWithoutLock_SharedByWorkers(t *testing.T) {
	producer, consumer1, client := setupTestQueue(t)
	consumer2 := NewConsumer(client, "test-queue", "")
	t.Cleanup(func() {
		consumer2.Close()
	})
	consumer1.SetPrefetchCount(1)
	consumer2.SetPrefetchCount(1)
	ctx := context.Background()

	// Две задачи в одной партиции без "!"
	for i := 1; i <= 2; i++ {
		task := &Task{
			ID:        fmt.Sprintf("task-%d", i),
			Partition: "shared",
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			Scheduled: time.Now().Add(-time.Second),
		}
		err := producer.Publish(ctx, task)
		require.NoError(t, err)
	}

	// Оба консьюмера могут одновременно получить задачи из партиции "shared"
	got1, err := consumer1.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got1)

	got2, err := consumer2.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got2)

	// Оба получили задачи — блокировки нет
	assert.Len(t, got1, 1)
	assert.Len(t, got2, 1)
	allIDs := []string{got1[0].ID, got2[0].ID}
	assert.Contains(t, allIDs, "task-1")
	assert.Contains(t, allIDs, "task-2")

	// Ключ блокировки не должен существовать для партиции без "!"
	exists, err := consumer1.redis.Exists(ctx, partitionLockKey("test-queue", "shared")).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists, "партиция без ! не должна иметь блокировку")

	consumer1.Ack(ctx, got1[0].ID)
	consumer2.Ack(ctx, got2[0].ID)
}

// TestQueue_Get_PartitionNoLock проверяет, что для партиции без "!" блокировка не создаётся.
func TestQueue_Get_PartitionNoLock(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	task := &Task{
		ID:        "task-1",
		Partition: "no-lock-partition",
		Payload:   []byte("test"),
		Scheduled: time.Now().Add(-time.Second),
	}
	err := producer.Publish(ctx, task)
	require.NoError(t, err)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got)
	assert.Equal(t, "task-1", got[0].ID)
	assert.Equal(t, "no-lock-partition", got[0].Partition)

	// Блокировка не должна существовать
	exists, err := consumer.redis.Exists(ctx, partitionLockKey("test-queue", "no-lock-partition")).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists)
}

// TestQueue_LockedPartition_SameWorkerMultipleTasks проверяет, что один воркер может взять
// несколько задач из заблокированной партиции, а другой — не может, пока первый не акнет все.
func TestQueue_LockedPartition_SameWorkerMultipleTasks(t *testing.T) {
	producer, consumer1, client := setupTestQueue(t)
	consumer2 := NewConsumer(client, "test-queue", "")
	t.Cleanup(func() {
		consumer2.Close()
	})
	consumer1.SetPrefetchCount(1)
	consumer2.SetPrefetchCount(1)
	ctx := context.Background()

	for i := 1; i <= 4; i++ {
		task := &Task{
			ID:        fmt.Sprintf("task-%d", i),
			Partition: "!locked",
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			Scheduled: time.Now().Add(-time.Second),
		}
		err := producer.Publish(ctx, task)
		require.NoError(t, err)
	}

	// consumer1 берёт 3 задачи подряд (без ack между ними)
	got1, err := consumer1.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got1, 1)
	assert.Equal(t, "task-1", got1[0].ID)

	got2, err := consumer1.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	assert.Equal(t, "task-2", got2[0].ID)

	got3, err := consumer1.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got3, 1)
	assert.Equal(t, "task-3", got3[0].ID)

	// consumer2 не может взять задачу — партиция заблокирована
	gotOther, err := consumer2.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, gotOther, "другой воркер не должен получить задачу из заблокированной партиции")

	// consumer1 акнит все 3 — блокировка снимается после последнего ack
	require.NoError(t, consumer1.Ack(ctx, "task-1"))
	require.NoError(t, consumer1.Ack(ctx, "task-2"))

	// consumer2 не может взять задачу — партиция заблокирована
	gotOther2, err := consumer2.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, gotOther2, "другой воркер не должен получить задачу из заблокированной партиции")

	// снимется блокировка
	require.NoError(t, consumer1.Ack(ctx, "task-3"))

	// Теперь consumer2 может взять оставшуюся задачу
	got4, err := consumer2.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got4, 1)
	assert.Equal(t, "task-4", got4[0].ID)
	require.NoError(t, consumer2.Ack(ctx, "task-4"))
}

// TestQueue_Priority_HigherFirst проверяет, что в одной партиции сообщения с большим приоритетом
// берутся в первую очередь.
func TestQueue_Priority_HigherFirst(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	tasks := []*Task{
		{ID: "task-low", Partition: "p1", Priority: 1, Payload: []byte("low"), Scheduled: time.Now().Add(-time.Second)},
		{ID: "task-high", Partition: "p1", Priority: 10, Payload: []byte("high"), Scheduled: time.Now().Add(-time.Second)},
		{ID: "task-mid", Partition: "p1", Priority: 5, Payload: []byte("mid"), Scheduled: time.Now().Add(-time.Second)},
	}
	err := producer.Publish(ctx, tasks...)
	require.NoError(t, err)

	// Ожидаемый порядок: high (10), mid (5), low (1)
	expectedOrder := []string{"task-high", "task-mid", "task-low"}
	var received []string
	for i := 0; i < 3; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, got)
		received = append(received, got[0].ID)
		err = consumer.Ack(ctx, got[0].ID)
		require.NoError(t, err)
	}
	assert.Equal(t, expectedOrder, received)
}

// --- Benchmarks ---

func setupBenchQueue(b *testing.B) (*Producer, *Consumer, *redis.Client) {
	cfg := RedisConfig{
		Addr:     "localhost:6379",
		Password: "",
		DB:       5,
	}

	client, err := newClient(cfg)
	if err != nil {
		b.Skipf("Redis not available: %v", err)
	}

	ctx := context.Background()
	client.FlushDB(ctx)

	producer := NewProducer(client, "bench-queue")
	consumer := NewConsumer(client, "bench-queue", "")
	b.Cleanup(func() {
		consumer.Close()
	})
	return producer, consumer, client
}

func BenchmarkPublish(b *testing.B) {
	producer, _, _ := setupBenchQueue(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = producer.Publish(ctx, &Task{
			ID:        fmt.Sprintf("t-%d", i),
			Payload:   []byte("bench-payload"),
			Scheduled: time.Now(),
		})
	}
}

func BenchmarkPublishBatch10(b *testing.B) {
	producer, _, _ := setupBenchQueue(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tasks := make([]*Task, 10)
		base := i * 10
		for j := 0; j < 10; j++ {
			tasks[j] = &Task{
				ID:        fmt.Sprintf("t-%d", base+j),
				Payload:   []byte("bench-payload"),
				Scheduled: time.Now(),
			}
		}
		_ = producer.Publish(ctx, tasks...)
	}
}

func BenchmarkPublishBatch100(b *testing.B) {
	producer, _, _ := setupBenchQueue(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tasks := make([]*Task, 100)
		base := i * 100
		for j := 0; j < 100; j++ {
			tasks[j] = &Task{
				ID:        fmt.Sprintf("t-%d", base+j),
				Payload:   []byte("bench-payload"),
				Scheduled: time.Now(),
			}
		}
		_ = producer.Publish(ctx, tasks...)
	}
}

func BenchmarkConsume(b *testing.B) {
	producer, consumer, _ := setupBenchQueue(b)
	ctx := context.Background()

	// Pre-fill the queue
	const batchSize = 100
	for i := 0; i < b.N; i += batchSize {
		count := batchSize
		if i+count > b.N {
			count = b.N - i
		}
		tasks := make([]*Task, count)
		for j := 0; j < count; j++ {
			tasks[j] = &Task{
				ID:        fmt.Sprintf("t-%d", i+j),
				Payload:   []byte("bench-payload"),
				Scheduled: time.Now().Add(-time.Second),
			}
		}
		_ = producer.Publish(ctx, tasks...)
	}

	b.ResetTimer()
	var consumed int64
	for consumed < int64(b.N) {
		tasks, _ := consumer.Get(ctx)
		for _, task := range tasks {
			if task != nil {
				consumer.Ack(ctx, task.ID)
				consumed++
			}
		}
	}
}

func BenchmarkConsumePrefetch(b *testing.B) {
	for _, prefetch := range []int{1, 5, 10, 20, 50} {
		b.Run(fmt.Sprintf("prefetch-%d", prefetch), func(b *testing.B) {
			producer, consumer, _ := setupBenchQueue(b)
			consumer.SetPrefetchCount(prefetch)
			ctx := context.Background()

			const batchSize = 500
			for i := 0; i < b.N; i += batchSize {
				count := batchSize
				if i+count > b.N {
					count = b.N - i
				}
				tasks := make([]*Task, count)
				for j := 0; j < count; j++ {
					tasks[j] = &Task{
						ID:        fmt.Sprintf("t-%d", i+j),
						Payload:   []byte("bench-payload"),
						Scheduled: time.Now().Add(-time.Second),
					}
				}
				_ = producer.Publish(ctx, tasks...)
			}

			b.ResetTimer()
			var consumed int64
			for consumed < int64(b.N) {
				tasks, _ := consumer.Get(ctx)
				for _, task := range tasks {
					if task != nil {
						consumer.Ack(ctx, task.ID)
						consumed++
					}
				}
			}
		})
	}
}

func BenchmarkConsumeParallel(b *testing.B) {
	for _, numConsumers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("consumers-%d", numConsumers), func(b *testing.B) {
			producer, _, client := setupBenchQueue(b)
			ctx := context.Background()

			consumers := make([]*Consumer, numConsumers)
			for i := range consumers {
				consumers[i] = NewConsumer(client, "bench-queue", "")
			}
			b.Cleanup(func() {
				for _, c := range consumers {
					c.Close()
				}
			})

			const batchSize = 100
			for i := 0; i < b.N; i += batchSize {
				count := batchSize
				if i+count > b.N {
					count = b.N - i
				}
				tasks := make([]*Task, count)
				for j := 0; j < count; j++ {
					tasks[j] = &Task{
						ID:        fmt.Sprintf("t-%d", i+j),
						Payload:   []byte("bench-payload"),
						Scheduled: time.Now().Add(-time.Second),
					}
				}
				_ = producer.Publish(ctx, tasks...)
			}

			var consumed atomic.Int64
			target := int64(b.N)

			b.ResetTimer()

			var wg sync.WaitGroup
			for _, c := range consumers {
				wg.Add(1)
				go func(c *Consumer) {
					defer wg.Done()
					for consumed.Load() < target {
						tasks, err := c.Get(ctx)
						if err != nil {
							continue
						}
						for _, task := range tasks {
							if task != nil {
								c.Ack(ctx, task.ID)
								consumed.Add(1)
							}
						}
					}
				}(c)
			}
			wg.Wait()
		})
	}
}
