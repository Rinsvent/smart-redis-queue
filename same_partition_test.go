package redisqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func publishPartitionTasks(t *testing.T, producer *Producer, partition, group string, n int, scheduled time.Time) {
	t.Helper()
	tasks := make([]*Task, 0, n)
	for i := 0; i < n; i++ {
		tasks = append(tasks, &Task{
			ID:        fmt.Sprintf("%s-%d", partition, i),
			Partition: partition,
			Groups:    []string{group},
			Payload:   []byte(partition),
			Scheduled: scheduled,
		})
	}
	require.NoError(t, producer.Publish(context.Background(), tasks...))
}

func partitionsOf(tasks []*Task) map[string]int {
	out := map[string]int{}
	for _, task := range tasks {
		out[task.Partition]++
	}
	return out
}

func TestQueue_SamePartitionBatch_TakesWholeBatchFromOnePartition(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	past := time.Now().Add(-time.Second)
	publishPartitionTasks(t, producer, "conn1", "read:conn1", 10, past)
	publishPartitionTasks(t, producer, "conn2", "read:conn2", 10, past)
	publishPartitionTasks(t, producer, "conn3", "read:conn3", 10, past)

	consumer.SetPrefetchCount(5)
	consumer.SetSamePartitionBatch(true)

	got, err := consumer.Get(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 5)
	assert.Len(t, partitionsOf(got), 1, "все задачи пачки из одной партиции")
}

func TestQueue_SamePartitionBatch_ShortBatchWithoutWaiting(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	publishPartitionTasks(t, producer, "conn1", "read:conn1", 3, time.Now().Add(-time.Second))
	require.NoError(t, producer.Publish(context.Background(),
		&Task{ID: "future-1", Partition: "conn1", Groups: []string{"read:conn1"}, Payload: []byte("f"), Scheduled: time.Now().Add(time.Hour)},
		&Task{ID: "future-2", Partition: "conn1", Groups: []string{"read:conn1"}, Payload: []byte("f"), Scheduled: time.Now().Add(time.Hour)},
	))

	consumer.SetPrefetchCount(5)
	consumer.SetSamePartitionBatch(true)

	got, err := consumer.Get(context.Background())
	require.NoError(t, err)
	assert.Len(t, got, 3, "отложенные задачи в пачку не попадают")
}

func TestQueue_SamePartitionBatch_SkipsBlockedGroup(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	past := time.Now().Add(-time.Second)
	publishPartitionTasks(t, producer, "conn1", "read:conn1", 5, past)
	publishPartitionTasks(t, producer, "conn2", "read:conn2", 5, past)
	require.NoError(t, consumer.BlockGroups(context.Background(), 5, "read:conn1"))

	consumer.SetPrefetchCount(5)
	consumer.SetSamePartitionBatch(true)

	got, err := consumer.Get(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 5)
	assert.Equal(t, map[string]int{"conn2": 5}, partitionsOf(got))
}

func TestQueue_SamePartitionBatch_OrderedPartitionKeepsOrder(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	base := time.Now().Add(-time.Minute)
	for i := 0; i < 4; i++ {
		require.NoError(t, producer.Publish(context.Background(), &Task{
			ID:        fmt.Sprintf("o-%d", i),
			Partition: "!conn1:chat",
			Groups:    []string{"conn1"},
			Payload:   []byte("o"),
			Scheduled: base.Add(time.Duration(i) * time.Second),
		}))
	}
	publishPartitionTasks(t, producer, "!conn2:chat", "conn2", 4, base)

	consumer.SetPrefetchCount(4)
	consumer.SetSamePartitionBatch(true)

	got, err := consumer.Get(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 4)
	require.Len(t, partitionsOf(got), 1)
	if got[0].Partition == "!conn1:chat" {
		for i, task := range got {
			assert.Equal(t, fmt.Sprintf("o-%d", i), task.ID)
		}
	}

	ctx := context.Background()
	for _, task := range got {
		require.NoError(t, consumer.Ack(ctx, task.ID, 0))
	}
	exists, err := rdb.Exists(ctx, "queue:test-queue:partition:"+got[0].Partition+":lock").Result()
	require.NoError(t, err)
	assert.Zero(t, exists, "lock снят после Ack всех задач пачки")
}

func TestQueue_SamePartitionBatch_ParallelBatchesDoNotOverlap(t *testing.T) {
	producer, first, rdb := setupTestQueue(t)
	publishPartitionTasks(t, producer, "conn1", "read:conn1", 10, time.Now().Add(-time.Second))

	second := NewConsumer(rdb, "test-queue", "")
	t.Cleanup(second.Close)
	for _, c := range []*Consumer{first, second} {
		c.SetPrefetchCount(5)
		c.SetSamePartitionBatch(true)
	}

	ctx := context.Background()
	a, err := first.Get(ctx)
	require.NoError(t, err)
	b, err := second.Get(ctx)
	require.NoError(t, err)
	require.Len(t, a, 5)
	require.Len(t, b, 5)

	seen := map[string]bool{}
	for _, task := range append(a, b...) {
		assert.False(t, seen[task.ID], "задача %s выдана дважды", task.ID)
		seen[task.ID] = true
	}
	assert.Len(t, seen, 10)
}

func TestQueue_SamePartitionBatch_DisabledMixesPartitions(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	past := time.Now().Add(-time.Second)
	for i := 0; i < 20; i++ {
		publishPartitionTasks(t, producer, fmt.Sprintf("conn%d", i), fmt.Sprintf("read:conn%d", i), 1, past)
	}

	consumer.SetPrefetchCount(5)

	got, err := consumer.Get(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 5)
	assert.Len(t, partitionsOf(got), 5, "без опции по задаче из разных партиций")
}

func groupUnlockAt(t *testing.T, consumer *Consumer, group string) float64 {
	t.Helper()
	score, err := consumer.redis.ZScore(context.Background(), "queue:test-queue:groups", group).Result()
	require.NoError(t, err)
	return score
}

func TestQueue_ExtendGroupBlocksOnly_RejectDoesNotShortenPause(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()
	publishPartitionTasks(t, producer, "conn1", "read:conn1", 2, time.Now().Add(-time.Second))
	consumer.SetPrefetchCount(2)
	consumer.SetExtendGroupBlocksOnly(true)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 2)

	require.NoError(t, consumer.Reject(ctx, got[0].ID, 30, "read:conn1"))
	long := groupUnlockAt(t, consumer, "read:conn1")
	require.NoError(t, consumer.Reject(ctx, got[1].ID, 5, "read:conn1"))

	assert.Equal(t, long, groupUnlockAt(t, consumer, "read:conn1"), "короткий отказ не сокращает паузу")
}

func TestQueue_ExtendGroupBlocksOnly_LongerPauseExtends(t *testing.T) {
	_, consumer, _ := setupTestQueue(t)
	ctx := context.Background()
	consumer.SetExtendGroupBlocksOnly(true)

	require.NoError(t, consumer.BlockGroups(ctx, 5, "read:conn1"))
	short := groupUnlockAt(t, consumer, "read:conn1")
	require.NoError(t, consumer.BlockGroups(ctx, 30, "read:conn1"))
	assert.Greater(t, groupUnlockAt(t, consumer, "read:conn1"), short)

	long := groupUnlockAt(t, consumer, "read:conn1")
	require.NoError(t, consumer.BlockGroups(ctx, 5, "read:conn1"))
	assert.Equal(t, long, groupUnlockAt(t, consumer, "read:conn1"))
}

func TestQueue_ExtendGroupBlocksOnly_DisabledOverwritesPause(t *testing.T) {
	_, consumer, _ := setupTestQueue(t)
	ctx := context.Background()

	require.NoError(t, consumer.BlockGroups(ctx, 30, "read:conn1"))
	long := groupUnlockAt(t, consumer, "read:conn1")
	require.NoError(t, consumer.BlockGroups(ctx, 5, "read:conn1"))

	assert.Less(t, groupUnlockAt(t, consumer, "read:conn1"), long, "без опции — прежнее поведение")
}
