package redisqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdmin_ListLockedPartitions(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	ctx := context.Background()
	admin := NewAdmin(rdb)
	past := time.Now().Add(-time.Second)
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "o1", Partition: "!ord", Payload: []byte("x"), Scheduled: past,
	}))
	consumer.SetPrefetchCount(1)
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	locked, err := admin.ListLockedPartitions(ctx, "test-queue")
	require.NoError(t, err)
	require.Len(t, locked, 1)
	assert.Equal(t, "!ord", locked[0].Partition)
	assert.True(t, locked[0].Locked)

	require.NoError(t, consumer.Ack(ctx, got[0].ID))
	locked, err = admin.ListLockedPartitions(ctx, "test-queue")
	require.NoError(t, err)
	assert.Empty(t, locked)
}

func TestAdmin_ScanTopPendingAndPoison(t *testing.T) {
	producer, _, rdb := setupTestQueue(t)
	ctx := context.Background()
	admin := NewAdmin(rdb)
	past := time.Now().Add(-time.Second)

	for i := 0; i < 5; i++ {
		require.NoError(t, producer.Publish(ctx, &Task{
			ID: fmt.Sprintf("hot-%d", i), Partition: "hot", Payload: []byte("x"), Scheduled: past,
		}))
	}
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "cold-0", Partition: "cold", Payload: []byte("x"), Scheduled: past,
	}))

	top, total, err := admin.ScanTopPending(ctx, "test-queue", 5)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, total, 2)
	require.NotEmpty(t, top)
	assert.Equal(t, "hot", top[0].Partition)
	assert.Equal(t, int64(5), top[0].Pending)

	// Отдельная очередь для poison, чтобы Get не мешали другие партиции
	p2 := NewProducer(rdb, "tox-q")
	c2 := NewConsumer(rdb, "tox-q", "")
	t.Cleanup(c2.Close)
	c2.SetPrefetchCount(1)
	require.NoError(t, p2.Publish(ctx, &Task{
		ID: "poison-1", Partition: "tox", Payload: []byte("x"), Scheduled: past,
	}))
	for i := 0; i < 3; i++ {
		got, err := c2.Get(ctx)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.NoError(t, c2.Reject(ctx, got[0].ID, 0))
	}
	poison, err := admin.ScanHighReject(ctx, "tox-q", 3, 10)
	require.NoError(t, err)
	require.Len(t, poison, 1)
	assert.Equal(t, "poison-1", poison[0].ID)
	assert.GreaterOrEqual(t, poison[0].RejectCount, 3)
}

func TestAdmin_ListRaceLocks(t *testing.T) {
	_, _, rdb := setupTestQueue(t)
	ctx := context.Background()
	admin := NewAdmin(rdb)
	require.NoError(t, rdb.HSet(ctx, "queue:test-queue:race:locks", "race!x", "task-1").Err())

	locks, err := admin.ListRaceLocks(ctx, "test-queue")
	require.NoError(t, err)
	require.Len(t, locks, 1)
	assert.Equal(t, "race!x", locks[0].Tag)
	assert.Equal(t, "task-1", locks[0].TaskID)
}

func TestAdmin_LockOwner(t *testing.T) {
	_, _, rdb := setupTestQueue(t)
	ctx := context.Background()
	admin := NewAdmin(rdb)

	_, err := admin.LockOwner(ctx, "test-queue", "")
	require.Error(t, err)

	owner, err := admin.LockOwner(ctx, "test-queue", "p1")
	require.NoError(t, err)
	assert.Empty(t, owner)

	require.NoError(t, rdb.Set(ctx, "queue:test-queue:partition:p1:lock", "consumer-9", 0).Err())
	owner, err = admin.LockOwner(ctx, "test-queue", "p1")
	require.NoError(t, err)
	assert.Equal(t, "consumer-9", owner)
}
