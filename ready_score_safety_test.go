package redisqueue

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Инварианты после ready-score: Get / block / unblock не теряют задачи и не
// снимают партицию, у которой ещё есть waiting (в т.ч. scheduled > now).

func TestSafety_FutureTasksSurviveIdleGets(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(5)
	ctx := context.Background()
	q := "test-queue"
	future := time.Now().Add(time.Hour)
	const n = 50

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("fut-%d", i)
		require.NoError(t, producer.Publish(ctx, &Task{
			ID: ids[i], Partition: fmt.Sprintf("!p:%d", i), Groups: []string{"g"},
			Payload: []byte("keep"), Scheduled: future,
		}))
	}

	for i := 0; i < 20; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		assert.Empty(t, got, "future must not be delivered")
	}

	nowMs := float64(time.Now().UnixMilli())
	if tm, err := rdb.Time(ctx).Result(); err == nil {
		nowMs = float64(tm.UnixMilli())
	}
	due, err := rdb.ZCount(ctx, "queue:"+q+":group:g:ready", "-inf", strconv.FormatInt(int64(nowMs), 10)).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), due)

	for i := 0; i < n; i++ {
		part := fmt.Sprintf("!p:%d", i)
		require.Equal(t, int64(1), rdb.Exists(ctx, "queue:"+q+":payload:"+ids[i]).Val(), ids[i])
		_, err := rdb.ZScore(ctx, "queue:"+q+":partition:"+part+":0", ids[i]).Result()
		require.NoError(t, err, "waiting zset %s", ids[i])
		_, err = rdb.ZScore(ctx, "queue:"+q+":group:g:ready", part).Result()
		require.NoError(t, err, "partition must stay in ready %s", part)
		assert.True(t, rdb.SIsMember(ctx, "queue:"+q+":partitions", part).Val(), part)
	}
}

func TestSafety_RejectDelay_TaskReturnsAndSurvivesIdleGets(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	q := "test-queue"
	part := "!chat:1"
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "t1", Partition: part, Groups: []string{"conn"},
		Payload: []byte("body"), Scheduled: time.Now().Add(-time.Second),
	}))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Reject(ctx, got[0].ID, 60))

	require.Equal(t, int64(1), rdb.Exists(ctx, "queue:"+q+":payload:t1").Val())
	_, err = rdb.ZScore(ctx, "queue:"+q+":partition:"+part+":0", "t1").Result()
	require.NoError(t, err)
	_, err = rdb.ZScore(ctx, "queue:"+q+":blocked", part).Result()
	require.NoError(t, err)
	readyScore, err := rdb.ZScore(ctx, "queue:"+q+":group:conn:ready", part).Result()
	require.NoError(t, err)
	assert.Greater(t, readyScore, float64(time.Now().UnixMilli()))

	for i := 0; i < 15; i++ {
		empty, err := consumer.Get(ctx)
		require.NoError(t, err)
		assert.Empty(t, empty)
	}

	require.Equal(t, int64(1), rdb.Exists(ctx, "queue:"+q+":payload:t1").Val())
	_, err = rdb.ZScore(ctx, "queue:"+q+":partition:"+part+":0", "t1").Result()
	require.NoError(t, err)
	assert.True(t, rdb.SIsMember(ctx, "queue:"+q+":partitions", part).Val())
	_, err = rdb.ZScore(ctx, "queue:"+q+":group:conn:ready", part).Result()
	require.NoError(t, err)
}

func TestSafety_BlockGroups_DoesNotDropTasks(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	q := "test-queue"
	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "a", Partition: "!a", Groups: []string{"g"}, Payload: []byte("1"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "b", Partition: "!b", Groups: []string{"g"}, Payload: []byte("2"), Scheduled: time.Now().Add(-time.Second)},
	))
	require.NoError(t, consumer.BlockGroups(ctx, 30, "g"))

	for i := 0; i < 10; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		assert.Empty(t, got)
	}

	for _, id := range []string{"a", "b"} {
		require.Equal(t, int64(1), rdb.Exists(ctx, "queue:"+q+":payload:"+id).Val(), id)
	}
	_, err := rdb.ZScore(ctx, "queue:"+q+":partition:!a:0", "a").Result()
	require.NoError(t, err)
	_, err = rdb.ZScore(ctx, "queue:"+q+":partition:!b:0", "b").Result()
	require.NoError(t, err)

	require.NoError(t, rdb.ZRem(ctx, "queue:"+q+":groups:blocked", "g").Err())
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	require.NoError(t, consumer.Ack(ctx, got2[0].ID, 0))
}

func TestSafety_BlockUnblockPartition_KeepsTask(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "safety-block-part"
	producer := NewProducer(rdb, q)
	consumer := newConsumer(rdb, q, "c1", false)
	consumer.SetPrefetchCount(1)
	t.Cleanup(func() { consumer.Close() })

	part := "!p1"
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "keep", Partition: part, Groups: []string{"g"},
		Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	require.NoError(t, admin.BlockPartition(ctx, q, part, 120))

	for i := 0; i < 10; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		assert.Empty(t, got)
	}
	require.Equal(t, int64(1), rdb.Exists(ctx, "queue:"+q+":payload:keep").Val())
	_, err := rdb.ZScore(ctx, "queue:"+q+":partition:"+part+":0", "keep").Result()
	require.NoError(t, err)

	require.NoError(t, admin.UnblockPartition(ctx, q, part))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "keep", got[0].ID)
	require.NoError(t, consumer.Ack(ctx, "keep", 0))
}

func TestSafety_DueAmongFutures_FuturesRemainAfterTake(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	q := "test-queue"
	future := time.Now().Add(time.Hour)

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "later", Partition: "!fut", Groups: []string{"g"}, Payload: []byte("f"), Scheduled: future},
		&Task{ID: "now", Partition: "!due", Groups: []string{"g"}, Payload: []byte("n"), Scheduled: time.Now().Add(-time.Second)},
	))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "now", got[0].ID)
	require.NoError(t, consumer.Ack(ctx, "now", 0))

	require.Equal(t, int64(1), rdb.Exists(ctx, "queue:"+q+":payload:later").Val())
	_, err = rdb.ZScore(ctx, "queue:"+q+":partition:!fut:0", "later").Result()
	require.NoError(t, err)
	score, err := rdb.ZScore(ctx, "queue:"+q+":group:g:ready", "!fut").Result()
	require.NoError(t, err)
	assert.Greater(t, score, float64(time.Now().UnixMilli()))

	empty, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, empty)
	require.Equal(t, int64(1), rdb.Exists(ctx, "queue:"+q+":payload:later").Val())
}

func TestSafety_MultiPriority_FutureHigh_KeepsLowReady(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	q := "test-queue"
	part := "mix"
	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "hi-fut", Partition: part, PartitionPriority: 10, Groups: []string{"g"},
			Payload: []byte("h"), Scheduled: time.Now().Add(time.Hour)},
		&Task{ID: "lo-now", Partition: part, PartitionPriority: 1, Groups: []string{"g"},
			Payload: []byte("l"), Scheduled: time.Now().Add(-time.Second)},
	))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "lo-now", got[0].ID)
	require.NoError(t, consumer.Ack(ctx, "lo-now", 0))

	require.Equal(t, int64(1), rdb.Exists(ctx, "queue:"+q+":payload:hi-fut").Val())
	_, err = rdb.ZScore(ctx, "queue:"+q+":partition:"+part+":10", "hi-fut").Result()
	require.NoError(t, err)
	score, err := rdb.ZScore(ctx, "queue:"+q+":group:g:ready", part).Result()
	require.NoError(t, err)
	assert.Greater(t, score, float64(time.Now().UnixMilli()), "readyAt = future high head")
}

func TestSafety_StaleScoreZero_Blocked_DoesNotDeleteTask(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	q := "test-queue"
	part := "!stale"
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "s1", Partition: part, Groups: []string{"g"},
		Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	unlock := float64(time.Now().Add(time.Hour).UnixMilli())
	require.NoError(t, rdb.ZAdd(ctx, "queue:"+q+":blocked", redis.Z{Score: unlock, Member: part}).Err())
	require.NoError(t, rdb.ZAdd(ctx, "queue:"+q+":group:g:ready", redis.Z{Score: 0, Member: part}).Err())

	for i := 0; i < 5; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		assert.Empty(t, got)
	}
	require.Equal(t, int64(1), rdb.Exists(ctx, "queue:"+q+":payload:s1").Val())
	_, err := rdb.ZScore(ctx, "queue:"+q+":partition:"+part+":0", "s1").Result()
	require.NoError(t, err)
	score, err := rdb.ZScore(ctx, "queue:"+q+":group:g:ready", part).Result()
	require.NoError(t, err)
	assert.Greater(t, score, float64(time.Now().UnixMilli()), "self-heal readyAt")
}

func TestSafety_RejectDelay_ThenUnblock_Delivers(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	admin := NewAdmin(rdb)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	q := "test-queue"
	part := "!u1"
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "r1", Partition: part, Groups: []string{"g"},
		Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Reject(ctx, got[0].ID, 3600))

	empty, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, empty)

	require.NoError(t, admin.UnblockPartition(ctx, q, part))
	again, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, again, 1)
	assert.Equal(t, "r1", again[0].ID)
	assert.Equal(t, []byte("x"), again[0].Payload)
	require.NoError(t, consumer.Ack(ctx, "r1", 0))
}
