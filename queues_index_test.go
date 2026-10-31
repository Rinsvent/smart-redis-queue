package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdmin_HasQueueIndex_AndListQueuesScan(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	ok, err := admin.HasQueueIndex(ctx)
	require.NoError(t, err)
	assert.False(t, ok)

	p := NewProducer(rdb, "idx-scan-q")
	require.NoError(t, p.Publish(ctx, &Task{
		ID: "t1", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	ok, err = admin.HasQueueIndex(ctx)
	require.NoError(t, err)
	assert.True(t, ok)

	scan, err := admin.ListQueuesScan(ctx)
	require.NoError(t, err)
	assert.Contains(t, scan, "idx-scan-q")
}

func TestQueueIndex_PublishAckUpdatesDepth(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	ctx := context.Background()
	admin := NewAdmin(rdb)

	depth, err := admin.QueueDepth(ctx, "test-queue")
	require.NoError(t, err)
	assert.Equal(t, int64(0), depth)

	past := time.Now().Add(-time.Second)
	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "a", Partition: "p1", Payload: []byte("1"), Scheduled: past},
		&Task{ID: "b", Partition: "p1", Payload: []byte("2"), Scheduled: past},
		&Task{ID: "c", Partition: "p2", Payload: []byte("3"), Scheduled: past},
	))
	depth, err = admin.QueueDepth(ctx, "test-queue")
	require.NoError(t, err)
	assert.Equal(t, int64(3), depth)

	consumer.SetPrefetchCount(2)
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 2)
	// Get не меняет total (ready→in-progress)
	depth, err = admin.QueueDepth(ctx, "test-queue")
	require.NoError(t, err)
	assert.Equal(t, int64(3), depth)

	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
	depth, err = admin.QueueDepth(ctx, "test-queue")
	require.NoError(t, err)
	assert.Equal(t, int64(2), depth)

	require.NoError(t, consumer.Reject(ctx, got[1].ID, 0))
	depth, err = admin.QueueDepth(ctx, "test-queue")
	require.NoError(t, err)
	assert.Equal(t, int64(2), depth, "Reject не меняет total")
}

func TestQueueDepthIfIndexed_Missing(t *testing.T) {
	_, _, rdb := setupTestQueue(t)
	ctx := context.Background()
	admin := NewAdmin(rdb)

	total, ok, err := admin.QueueDepthIfIndexed(ctx, "no-such-queue")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, int64(0), total)

	total, ok, err = admin.QueueDepthIfIndexed(ctx, "test-queue")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(0), total)
}

func TestQueueIndex_ListByDepthAndRebuild(t *testing.T) {
	_, _, rdb := setupTestQueue(t)
	ctx := context.Background()
	admin := NewAdmin(rdb)

	pSmall := NewProducer(rdb, "q-small")
	pBig := NewProducer(rdb, "q-big")
	past := time.Now().Add(-time.Second)
	require.NoError(t, pSmall.Publish(ctx, &Task{ID: "s1", Partition: "p", Payload: []byte("x"), Scheduled: past}))
	require.NoError(t, pBig.Publish(ctx,
		&Task{ID: "b1", Partition: "p", Payload: []byte("x"), Scheduled: past},
		&Task{ID: "b2", Partition: "p", Payload: []byte("x"), Scheduled: past},
		&Task{ID: "b3", Partition: "p", Payload: []byte("x"), Scheduled: past},
	))

	byDepth, err := admin.ListQueueIndex(ctx, true)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(byDepth), 2)
	assert.Equal(t, "q-big", byDepth[0].Name)
	assert.Equal(t, int64(3), byDepth[0].Total)

	require.NoError(t, rdb.ZAdd(ctx, QueuesIndexKey, redis.Z{Score: 99, Member: "q-big"}).Err())
	n, err := admin.RebuildQueueDepth(ctx, "q-big")
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
	depth, err := admin.QueueDepth(ctx, "q-big")
	require.NoError(t, err)
	assert.Equal(t, int64(3), depth)
}

func TestAdmin_ListBlockedPartitionsAndGroups(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	ctx := context.Background()
	admin := NewAdmin(rdb)
	past := time.Now().Add(-time.Second)
	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "1", Partition: "p1", Groups: []string{"g1"}, Payload: []byte("x"), Scheduled: past},
		&Task{ID: "2", Partition: "p2", Groups: []string{"g2"}, Payload: []byte("x"), Scheduled: past},
	))
	require.NoError(t, admin.BlockPartition(ctx, "test-queue", "p1", 30))
	require.NoError(t, consumer.BlockGroups(ctx, 30, "g2"))

	parts, err := admin.ListBlockedPartitions(ctx, "test-queue")
	require.NoError(t, err)
	require.Len(t, parts, 1)
	assert.Equal(t, "p1", parts[0].Partition)

	groups, err := admin.ListBlockedGroups(ctx, "test-queue")
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "g2", groups[0].Name)
	assert.True(t, groups[0].Blocked)
}
