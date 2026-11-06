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

// Legacy cleanup must not ZREM shared groups while a qp-lane still has ready work
// for the same group (future/paused bot reply while broadcast partition empties).
func TestQueue_QueuePriority_LaneTaskSurvivesLegacyGroupCleanup(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	now := time.Now().Add(-time.Second)

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "bcast", Partition: "!c:1", Groups: []string{"conn:1"}, Payload: []byte("b"), Scheduled: now},
		&Task{ID: "bot", Partition: "!c:2", Groups: []string{"conn:1"}, QueuePriority: 10, Payload: []byte("r"), Scheduled: time.Now().Add(300 * time.Millisecond)},
	))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, "bcast", got[0].ID)
	require.NoError(t, consumer.Ack(ctx, "bcast", 0))

	empty, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Empty(t, empty)

	// group must stay while qp10 ready still references conn:1
	score, err := rdb.ZScore(ctx, "queue:test-queue:groups", "conn:1").Result()
	require.NoError(t, err)
	_ = score

	time.Sleep(400 * time.Millisecond)
	again, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, again, 1)
	require.Equal(t, "bot", again[0].ID)
}

// Emptying the last legacy task on a partition must keep partition:{p}:groups
// while a qp-lane still has that partition (Reject must restore into conn:1, not _default).
func TestQueue_QueuePriority_SharedPartitionGroupsSurviveLegacyCleanup(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	admin := NewAdmin(rdb)
	ctx := context.Background()
	now := time.Now().Add(-time.Second)
	part := "chat:1"

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "legacy", Partition: part, Groups: []string{"conn:1"}, Payload: []byte("l"), Scheduled: now},
		&Task{ID: "qp10", Partition: part, Groups: []string{"conn:1"}, QueuePriority: 10, Payload: []byte("q"), Scheduled: now},
	))

	st, err := admin.Remove(ctx, "test-queue", "legacy")
	require.NoError(t, err)
	require.Equal(t, RemovalStatusRemoved, st.Status)

	members, err := rdb.SMembers(ctx, "queue:test-queue:partition:"+part+":groups").Result()
	require.NoError(t, err)
	assert.Contains(t, members, "conn:1")

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "qp10", got[0].ID)
	require.NoError(t, consumer.Reject(ctx, got[0].ID, 0))

	assert.Equal(t, int64(1), rdb.Exists(ctx, "queue:test-queue:qp10:group:conn:1:ready").Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:test-queue:qp10:group:_default:ready").Val())
}
