package redisqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const hotTestQueue = "test-queue"

// publishCrowd — n партиций группы с задачами низкого приоритета.
func publishCrowd(t *testing.T, producer *Producer, group string, n int, priority int) {
	t.Helper()
	tasks := make([]*Task, 0, n)
	for i := 0; i < n; i++ {
		tasks = append(tasks, &Task{
			ID:        fmt.Sprintf("low-%d", i),
			Partition: fmt.Sprintf("!low-%d", i),
			Groups:    []string{group},
			Priority:  priority,
			Payload:   []byte("low"),
			Scheduled: time.Now().Add(-time.Second),
		})
	}
	require.NoError(t, producer.Publish(context.Background(), tasks...))
}

func publishHot(t *testing.T, producer *Producer, id, group string) {
	t.Helper()
	require.NoError(t, producer.Publish(context.Background(), &Task{
		ID:        id,
		Partition: "!" + id,
		Groups:    []string{group},
		Priority:  10000,
		Payload:   []byte("hot"),
		Scheduled: time.Now().Add(-time.Second),
	}))
}

func TestHotPriority_HighPriorityPartitionFirst(t *testing.T) {
	producer, consumer, client := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	require.NoError(t, SetHotPriority(ctx, client, hotTestQueue, 10000))
	publishCrowd(t, producer, "conn:1", 300, 4000)
	publishHot(t, producer, "bot-1", "conn:1")

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "bot-1", got[0].ID, "задача выше порога берётся раньше партиций рассылки")
}

func TestHotPriority_DisabledDoesNotTrackHot(t *testing.T) {
	producer, _, client := setupTestQueue(t)
	ctx := context.Background()
	require.NoError(t, SetHotPriority(ctx, client, hotTestQueue, 0))
	publishHot(t, producer, "bot-1", "conn:1")

	assert.Zero(t, client.Exists(ctx, "queue:"+hotTestQueue+":group:conn:1:hot").Val(), "без порога hot не ведётся")
}

func TestHotPriority_RejectReturnsTaskToHot(t *testing.T) {
	producer, consumer, client := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	require.NoError(t, SetHotPriority(ctx, client, hotTestQueue, 10000))
	publishCrowd(t, producer, "conn:1", 300, 4000)
	publishHot(t, producer, "bot-1", "conn:1")
	hotKey := "queue:" + hotTestQueue + ":group:conn:1:hot"

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, "bot-1", got[0].ID)
	// Партиция могла уйти из hot, пока задача в работе (обход увидел её пустой).
	require.NoError(t, client.ZRem(ctx, hotKey, "!bot-1").Err())

	// Парковка (Reject с паузой) возвращает партицию в hot.
	require.NoError(t, consumer.Reject(ctx, "bot-1", 0.2))
	_, zerr := client.ZScore(ctx, hotKey, "!bot-1").Result()
	require.NoError(t, zerr, "Reject возвращает партицию в hot")
	time.Sleep(300 * time.Millisecond)

	again, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, again, 1)
	assert.Equal(t, "bot-1", again[0].ID, "после паузы задача снова берётся раньше рассылки")
}

func TestHotPriority_PartitionWithoutHotTaskLeavesHot(t *testing.T) {
	producer, consumer, client := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	require.NoError(t, SetHotPriority(ctx, client, hotTestQueue, 10000))
	publishHot(t, producer, "bot-1", "conn:1")
	hotKey := "queue:" + hotTestQueue + ":group:conn:1:hot"
	require.EqualValues(t, 1, client.ZCard(ctx, hotKey).Val())

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	empty, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, empty)
	assert.Zero(t, client.ZCard(ctx, hotKey).Val(), "пустая партиция уходит из hot")
}

func TestHotPriority_LowerPriorityTasksStillServed(t *testing.T) {
	producer, consumer, client := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	require.NoError(t, SetHotPriority(ctx, client, hotTestQueue, 10000))
	publishCrowd(t, producer, "conn:1", 5, 4000)

	seen := 0
	for i := 0; i < 5; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
		seen++
	}
	assert.Equal(t, 5, seen, "без hot-задач обход ready работает как раньше")
}

func publishHotAt(t *testing.T, producer *Producer, id, partition, group string, scheduled time.Time) {
	t.Helper()
	require.NoError(t, producer.Publish(context.Background(), &Task{
		ID:        id,
		Partition: partition,
		Groups:    []string{group},
		Priority:  10000,
		Payload:   []byte("hot"),
		Scheduled: scheduled,
	}))
}

func TestHotPriority_FutureHotPartitionsDoNotHideReadyOne(t *testing.T) {
	producer, consumer, client := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	require.NoError(t, SetHotPriority(ctx, client, hotTestQueue, 10000))
	publishCrowd(t, producer, "conn:1", 300, 4000)
	future := time.Now().Add(time.Hour)
	for i := 0; i < 300; i++ {
		publishHotAt(t, producer, fmt.Sprintf("later-%d", i), fmt.Sprintf("!later-%d", i), "conn:1", future)
	}
	publishHot(t, producer, "bot-1", "conn:1")
	hotKey := "queue:" + hotTestQueue + ":group:conn:1:hot"

	score, err := client.ZScore(ctx, hotKey, "!later-0").Result()
	require.NoError(t, err)
	assert.InDelta(t, float64(future.UnixMilli()), score, 1000, "score hot = время задачи, а не 0")

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "bot-1", got[0].ID, "отложенные hot-задачи не закрывают готовую hot-партицию")
}

func TestHotPriority_LockedHotPartitionSkippedAndKept(t *testing.T) {
	producer, consumer, client := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	other := NewConsumer(client, hotTestQueue, "other")
	other.SetPrefetchCount(1)
	t.Cleanup(func() { other.Close() })
	ctx := context.Background()
	require.NoError(t, SetHotPriority(ctx, client, hotTestQueue, 10000))
	now := time.Now().Add(-time.Second)
	publishHotAt(t, producer, "bot-1", "!bot-1", "conn:1", now)

	held, err := other.Get(ctx)
	require.NoError(t, err)
	require.Len(t, held, 1)
	require.Equal(t, "bot-1", held[0].ID)

	// Ordered-партиция !bot-1 под lock другого консьюмера, в ней новая hot-задача.
	publishHotAt(t, producer, "bot-1b", "!bot-1", "conn:1", now)
	publishHot(t, producer, "bot-2", "conn:1")
	publishCrowd(t, producer, "conn:1", 300, 4000)
	hotKey := "queue:" + hotTestQueue + ":group:conn:1:hot"

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "bot-2", got[0].ID, "партиция под чужим lock пропускается")
	_, zerr := client.ZScore(ctx, hotKey, "!bot-1").Result()
	require.NoError(t, zerr, "партиция под lock остаётся в hot")

	require.NoError(t, consumer.Ack(ctx, "bot-2", 0))
	require.NoError(t, other.Ack(ctx, "bot-1", 0))

	again, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, again, 1)
	assert.Equal(t, "bot-1b", again[0].ID, "после снятия lock hot-задача берётся раньше рассылки")
}

func TestHotPriority_MoreHotPartitionsThanWindow(t *testing.T) {
	producer, consumer, client := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	require.NoError(t, SetHotPriority(ctx, client, hotTestQueue, 10000))
	publishCrowd(t, producer, "conn:1", 300, 4000)
	const hot = 300 // больше окна 256 за один Get
	for i := 0; i < hot; i++ {
		publishHot(t, producer, fmt.Sprintf("bot-%d", i), "conn:1")
	}

	for i := 0; i < hot; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Containsf(t, got[0].ID, "bot-", "Get %d: hot-партиции раньше рассылки и за пределами окна 256", i)
		require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
	}

	// Ack не трогает hot: пустые партиции снимает следующий Get и переходит к рассылке.
	low, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, low, 1)
	assert.Contains(t, low[0].ID, "low-")
	assert.Zero(t, client.ZCard(ctx, "queue:"+hotTestQueue+":group:conn:1:hot").Val(), "пустые партиции ушли из hot")
}
