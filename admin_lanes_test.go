package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdmin_Lanes_ListPartitionsAndGroupsSeeQP(t *testing.T) {
	producer, _, rdb := setupTestQueue(t)
	admin := NewAdmin(rdb)
	ctx := context.Background()
	q := "test-queue"

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "leg", Partition: "chat-leg", Groups: []string{"conn:1"}, Payload: []byte("0"),
			Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "hi", Partition: "chat-hi", Groups: []string{"conn:1"}, QueuePriority: 10, Payload: []byte("10"),
			Scheduled: time.Now().Add(-time.Second)},
	))

	parts, total, err := admin.ListPartitions(ctx, q, "", 100)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	names := map[string]bool{}
	for _, p := range parts {
		names[p.Partition] = true
		if p.Partition == "chat-hi" {
			assert.Greater(t, p.Pending, int64(0))
		}
	}
	assert.True(t, names["chat-leg"])
	assert.True(t, names["chat-hi"], "qp lane partition must appear in ListPartitions")

	groups, _, err := admin.ListGroups(ctx, q, "", 100)
	require.NoError(t, err)
	var conn *GroupStats
	for i := range groups {
		if groups[i].Name == "conn:1" {
			conn = &groups[i]
			break
		}
	}
	require.NotNil(t, conn)
	assert.GreaterOrEqual(t, conn.Ready, int64(2), "ready must sum legacy + qp lanes")

	ov, err := admin.QueueOverview(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, 2, ov.PartitionTotal)
	assert.GreaterOrEqual(t, ov.TotalPending, int64(2))
}

func TestAdmin_QueueSnapshot_PausedAndOldest(t *testing.T) {
	producer, _, rdb := setupTestQueue(t)
	admin := NewAdmin(rdb)
	ctx := context.Background()
	q := "test-queue"

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "old-hi", Partition: "c1", Groups: []string{"tok:a"}, QueuePriority: 5,
			PartitionPriority: 1, Payload: []byte("a"), Scheduled: time.Now().Add(-2 * time.Second)},
		&Task{ID: "new-hi", Partition: "c2", Groups: []string{"tok:a"}, QueuePriority: 5,
			PartitionPriority: 1, Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "low", Partition: "c3", Groups: []string{"tok:a"}, QueuePriority: 0,
			Payload: []byte("c"), Scheduled: time.Now().Add(-3 * time.Second)},
	))

	require.NoError(t, admin.BlockGroups(ctx, q, 60, "tok:a"))
	snap, err := admin.QueueSnapshot(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, 3, snap.PartitionTotal)
	assert.Equal(t, 3, snap.PausedCount, "group pause → all chats paused")
	assert.Equal(t, 0, snap.ActiveChats)
	require.NotEmpty(t, snap.Groups)
	var g *GroupSnapshot
	for i := range snap.Groups {
		if snap.Groups[i].Name == "tok:a" {
			g = &snap.Groups[i]
			break
		}
	}
	require.NotNil(t, g)
	assert.True(t, g.Blocked)
	assert.True(t, g.HasWork)

	// снять паузу группы
	require.NoError(t, rdb.ZAdd(ctx, "queue:"+q+":groups", redis.Z{Score: 0, Member: "tok:a"}).Err())

	snap2, err := admin.QueueSnapshot(ctx, q)
	require.NoError(t, err)
	require.NotNil(t, snap2.OldestReady)
	assert.Equal(t, "old-hi", snap2.OldestReady.ID, "higher qp + older score")
	assert.Equal(t, 5, snap2.OldestReady.QueuePriority)
	assert.Greater(t, snap2.ActiveChats, 0)
}

// Multi-group partition must match Get admission: any sibling group blocked → not ready,
// even when discovered via another free group's :ready set.
func TestAdmin_QueueSnapshot_OldestReady_MultiGroupAdmission(t *testing.T) {
	producer, _, rdb := setupTestQueue(t)
	admin := NewAdmin(rdb)
	ctx := context.Background()
	q := "test-queue"

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "multi", Partition: "c-multi", Groups: []string{"tok:a", "tok:b"}, QueuePriority: 5,
			PartitionPriority: 1, Payload: []byte("m"), Scheduled: time.Now().Add(-2 * time.Second)},
		&Task{ID: "solo", Partition: "c-solo", Groups: []string{"tok:b"}, QueuePriority: 5,
			PartitionPriority: 1, Payload: []byte("s"), Scheduled: time.Now().Add(-time.Second)},
	))

	require.NoError(t, admin.BlockGroups(ctx, q, 60, "tok:a"))

	snap, err := admin.QueueSnapshot(ctx, q)
	require.NoError(t, err)
	require.NotNil(t, snap.OldestReady)
	assert.Equal(t, "solo", snap.OldestReady.ID, "multi-group chat paused via tok:a must not win via tok:b")

	require.NoError(t, rdb.ZAdd(ctx, "queue:"+q+":groups", redis.Z{Score: 0, Member: "tok:a"}).Err())
	snap2, err := admin.QueueSnapshot(ctx, q)
	require.NoError(t, err)
	require.NotNil(t, snap2.OldestReady)
	assert.Equal(t, "multi", snap2.OldestReady.ID, "after unblock, older multi-group head wins")
}
