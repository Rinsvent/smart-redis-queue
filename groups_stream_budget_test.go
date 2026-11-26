package redisqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueue_BlockGroups_DoesNotBlockOtherQP — BlockGroups → legacy groups:blocked;
// ответы бота в qp10000 продолжают идти (prod-паттерн Messenger).
func TestQueue_BlockGroups_DoesNotBlockOtherQP(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	q := consumer.queueName
	const group = "conn:1"

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "mass-1", Partition: "!c:1", Groups: []string{group}, QueuePriority: 0, Payload: []byte("m"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "bot-1", Partition: "!c:1", Groups: []string{group}, QueuePriority: 10000, Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second)},
	))

	require.NoError(t, consumer.BlockGroups(ctx, 30, group))

	score, err := rdb.ZScore(ctx, "queue:"+q+":groups:blocked", group).Result()
	require.NoError(t, err)
	assert.Greater(t, score, float64(time.Now().UnixMilli()))
	gScore, err := rdb.ZScore(ctx, "queue:"+q+":groups", group).Result()
	require.NoError(t, err)
	assert.LessOrEqual(t, gScore, float64(time.Now().UnixMilli()), "catalog groups stays free")

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "bot-1", got[0].ID, "qp10000 must still deliver under legacy-stream block")
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2, "mass stream still blocked")
}

// TestQueue_Reject_BlockGroups_UsesTaskLane — Reject(…, group) пишет в pref задачи.
func TestQueue_Reject_BlockGroups_UsesTaskLane(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	q := consumer.queueName
	const group = "conn:x"

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "m1", Partition: "!p:1", Groups: []string{group}, QueuePriority: 0, Payload: []byte("m"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "b1", Partition: "!p:2", Groups: []string{group}, QueuePriority: 5, Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second)},
	))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "b1", got[0].ID)
	require.NoError(t, consumer.Reject(ctx, got[0].ID, 30, group))

	_, err = rdb.ZScore(ctx, "queue:"+q+":qp5:groups:blocked", group).Result()
	require.NoError(t, err)

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	assert.Equal(t, "m1", got2[0].ID, "legacy mass stream still free")
	require.NoError(t, consumer.Ack(ctx, got2[0].ID, 0))
}

func TestQueue_PartitionScanBudget_AbandonsLargeParkedGroup(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	consumer.SetPartitionScanBudget(-1)
	ctx := context.Background()
	const group = "conn"
	const parts = 40

	tasks := make([]*Task, 0, parts)
	for i := 0; i < parts; i++ {
		tasks = append(tasks, &Task{
			ID:        fmt.Sprintf("t-%d", i),
			Partition: fmt.Sprintf("!%s:%d", group, i),
			Groups:    []string{group},
			Payload:   []byte("x"),
			Scheduled: time.Now().Add(-time.Second),
		})
	}
	require.NoError(t, producer.Publish(ctx, tasks...))

	parked := 0
	for parked < parts {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, got, "empty while parking %d/%d", parked, parts)
		for _, task := range got {
			require.NoError(t, consumer.Reject(ctx, task.ID, 3600))
			parked++
		}
	}

	consumer.SetPartitionScanBudget(8)
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.Greater(t, consumer.LastPartitionBudgetHits(), int64(0), "budget must trip on large parked group")
}
