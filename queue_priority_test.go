package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// High QueuePriority in another group beats low/default even if that group would be scanned later.
func TestQueue_QueuePriority_CrossGroupWins(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	err := producer.Publish(ctx,
		&Task{ID: "low-g1", Partition: "p1", Groups: []string{"g1"}, QueuePriority: 0, Payload: []byte("low"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "high-g2", Partition: "p2", Groups: []string{"g2"}, QueuePriority: 10, Payload: []byte("high"), Scheduled: time.Now().Add(-time.Second)},
	)
	require.NoError(t, err)

	assert.Equal(t, int64(1), rdb.Exists(ctx, "queue:test-queue:qpriority:high-g2").Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:test-queue:qpriority:low-g1").Val())
	assert.Equal(t, int64(1), rdb.Exists(ctx, "queue:test-queue:qp10:partition:p2:0").Val())
	assert.Equal(t, int64(1), rdb.Exists(ctx, "queue:test-queue:partition:p1:0").Val())

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "high-g2", got[0].ID)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	got, err = consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "low-g1", got[0].ID)
}

// After qp lanes are drained, Get falls back to legacy (qp=0) keys.
func TestQueue_QueuePriority_FallbackToLegacy(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "legacy", Partition: "p0", Payload: []byte("0"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "qp5", Partition: "p5", QueuePriority: 5, Payload: []byte("5"), Scheduled: time.Now().Add(-time.Second)},
	))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "qp5", got[0].ID)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	// lane empty → member removed from queue:priorities on next Get miss or after SCARD==0
	got, err = consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "legacy", got[0].ID)
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:test-queue:qpriority:qp5").Val())
}

// Ordered lock is shared across qp lanes for the same partition.
func TestQueue_QueuePriority_SharedOrderedLock(t *testing.T) {
	producer, consumer1, rdb := setupTestQueue(t)
	consumer1.SetPrefetchCount(1)
	consumer2 := NewConsumer(rdb, "test-queue", "")
	consumer2.SetPrefetchCount(1)
	t.Cleanup(func() { consumer2.Close() })
	ctx := context.Background()

	part := "!shared"
	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "qp1-task", Partition: part, QueuePriority: 1, Payload: []byte("1"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "legacy-task", Partition: part, QueuePriority: 0, Payload: []byte("0"), Scheduled: time.Now().Add(-time.Second)},
	))

	got1, err := consumer1.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got1, 1)
	assert.Equal(t, "qp1-task", got1[0].ID)

	// second consumer cannot take same ! partition while locked
	got2, err := consumer2.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)

	require.NoError(t, consumer1.Ack(ctx, got1[0].ID, 0))

	got2, err = consumer2.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	assert.Equal(t, "legacy-task", got2[0].ID)
}

func TestQueue_QueuePriority_RejectReturnsToSameLane(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "qp3", Partition: "p", QueuePriority: 3, Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second)},
	))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Reject(ctx, got[0].ID, 0))

	assert.Equal(t, int64(1), rdb.Exists(ctx, "queue:test-queue:qp3:partition:p:0").Val())
	assert.Equal(t, "3", rdb.Get(ctx, "queue:test-queue:qpriority:qp3").Val())

	got, err = consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "qp3", got[0].ID)
}
