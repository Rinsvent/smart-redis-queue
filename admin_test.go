package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redis/go-redis/v9"
)

func setupAdminTest(t *testing.T) (*Admin, *redis.Client) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       5,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	ctx2 := context.Background()
	rdb.FlushDB(ctx2)
	return NewAdmin(rdb), rdb
}

func TestAdmin_ListQueues(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "admin-test-queue")
	_ = producer.Publish(ctx, &Task{
		ID:        "t1",
		Payload:   []byte("x"),
		Scheduled: time.Now(),
	})

	queues, err := admin.ListQueues(ctx)
	require.NoError(t, err)
	assert.Contains(t, queues, "admin-test-queue")
}

func TestAdmin_Inspect(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "inspect-queue")
	_ = producer.Publish(ctx,
		&Task{ID: "t1", Partition: "base", Payload: []byte("a"), Scheduled: time.Now()},
		&Task{ID: "t2", Partition: "p1", Payload: []byte("b"), Scheduled: time.Now()},
	)

	statsList, err := admin.Inspect(ctx, "inspect-queue", "")
	require.NoError(t, err)
	require.Len(t, statsList, 1)
	stats := statsList[0]
	assert.Equal(t, "inspect-queue", stats.QueueName)
	assert.Equal(t, int64(2), stats.TotalPending)
	assert.Len(t, stats.Partitions, 2)
}

func TestAdmin_Purge(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "purge-queue")
	_ = producer.Publish(ctx, &Task{ID: "t1", Payload: []byte("x"), Scheduled: time.Now()})

	n, err := admin.Purge(ctx, "purge-queue", "")
	require.NoError(t, err)
	assert.Greater(t, n, 0)

	queues, _ := admin.ListQueues(ctx)
	assert.NotContains(t, queues, "purge-queue")
}

func TestAdmin_PurgePartition(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "purge-part-queue")
	_ = producer.Publish(ctx,
		&Task{ID: "t1", Partition: "base", Payload: []byte("a"), Scheduled: time.Now()},
		&Task{ID: "t2", Partition: "p1", Payload: []byte("b"), Scheduled: time.Now()},
	)

	n, err := admin.Purge(ctx, "purge-part-queue", "p1")
	require.NoError(t, err)
	assert.Greater(t, n, 0)

	statsList, _ := admin.Inspect(ctx, "purge-part-queue", "")
	require.Len(t, statsList, 1)
	assert.Equal(t, int64(1), statsList[0].TotalPending)
}

func TestAdmin_Retry(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "retry-queue")
	consumer := newConsumer(rdb, "retry-queue", "", false)
	consumer.SetPrefetchCount(1)

	_ = producer.Publish(ctx, &Task{ID: "t1", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second)})
	tasks, _ := consumer.Get(ctx)
	require.Len(t, tasks, 1)

	n, err := admin.Retry(ctx, "retry-queue")
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	tasks2, _ := consumer.Get(ctx)
	require.Len(t, tasks2, 1)
	assert.Equal(t, "t1", tasks2[0].ID)
}
