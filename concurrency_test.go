package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRaceTag_Helpers(t *testing.T) {
	assert.True(t, HasRaceTagPrefix(RaceTagPrefix+"file:abc"))
	assert.False(t, HasRaceTagPrefix("file:abc"))
}

func TestConcurrency_OptInOff_NoLocks(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	// concurrency выкл
	ctx := context.Background()
	tag := RaceTagPrefix + "file:1"
	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "a", Partition: "p1", Payload: []byte("a"), Tags: []string{tag},
			Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "b", Partition: "p2", Payload: []byte("b"), Tags: []string{tag},
			Scheduled: time.Now().Add(-time.Second)},
	))
	assert.Equal(t, int64(1), rdb.Exists(ctx, "queue:test-queue:race:tags:a").Val())

	got1, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got1, 1)
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1, "без opt-in оба Get проходят параллельно")
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:test-queue:race:locks").Val())
	require.NoError(t, consumer.Ack(ctx, got1[0].ID, 0))
	require.NoError(t, consumer.Ack(ctx, got2[0].ID, 0))
}

func TestConcurrency_BlocksOtherPartition(t *testing.T) {
	producer, consumerA, rdb := setupTestQueue(t)
	consumerB := NewConsumer(consumerA.redis, "test-queue", "b")
	t.Cleanup(func() { consumerB.Close() })
	consumerA.SetPrefetchCount(1)
	consumerB.SetPrefetchCount(1)
	consumerA.SetConcurrencyTags(true)
	consumerB.SetConcurrencyTags(true)
	ctx := context.Background()
	tag := RaceTagPrefix + "file:xyz"

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "t1", Partition: "!c:1", Payload: []byte("1"), Tags: []string{tag},
			Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "t2", Partition: "!c:2", Payload: []byte("2"), Tags: []string{tag},
			Scheduled: time.Now().Add(-time.Second)},
	))

	gotA, err := consumerA.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotA, 1)
	holder, err := rdb.HGet(ctx, "queue:test-queue:race:locks", tag).Result()
	require.NoError(t, err)
	assert.Equal(t, gotA[0].ID, holder)

	gotB, err := consumerB.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, gotB, "вторая партиция ждёт race lock")

	require.NoError(t, consumerA.Ack(ctx, gotA[0].ID, 0))
	assert.False(t, rdb.HExists(ctx, "queue:test-queue:race:locks", tag).Val())

	gotB, err = consumerB.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotB, 1)
	require.NoError(t, consumerB.Ack(ctx, gotB[0].ID, 0))
}

func TestConcurrency_RejectReleasesLock(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	consumer.SetConcurrencyTags(true)
	ctx := context.Background()
	tag := RaceTagPrefix + "file:rej"

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "r1", Partition: "p", Payload: []byte("x"), Tags: []string{tag},
		Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.True(t, rdb.HExists(ctx, "queue:test-queue:race:locks", tag).Val())

	require.NoError(t, consumer.Reject(ctx, got[0].ID, 0))
	assert.False(t, rdb.HExists(ctx, "queue:test-queue:race:locks", tag).Val())
	assert.Equal(t, int64(1), rdb.Exists(ctx, "queue:test-queue:race:tags:r1").Val())

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	require.NoError(t, consumer.Ack(ctx, got2[0].ID, 0))
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:test-queue:race:tags:r1").Val())
}

func TestConcurrency_MultiTags_AND(t *testing.T) {
	producer, consumerA, rdb := setupTestQueue(t)
	consumerB := NewConsumer(consumerA.redis, "test-queue", "b2")
	t.Cleanup(func() { consumerB.Close() })
	consumerA.SetPrefetchCount(1)
	consumerB.SetPrefetchCount(1)
	consumerA.SetConcurrencyTags(true)
	consumerB.SetConcurrencyTags(true)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "m1", Partition: "p1", Payload: []byte("1"),
		Tags:      []string{RaceTagPrefix + "a", RaceTagPrefix + "b"},
		Scheduled: time.Now().Add(-time.Second),
	}))
	gotA, err := consumerA.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotA, 1)
	require.Equal(t, "m1", gotA[0].ID)
	assert.Equal(t, int64(2), rdb.HLen(ctx, "queue:test-queue:race:locks").Val())

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "m2", Partition: "p2", Payload: []byte("2"),
		Tags:      []string{RaceTagPrefix + "b"},
		Scheduled: time.Now().Add(-time.Second),
	}))
	gotB, err := consumerB.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, gotB, "race!b занят m1")

	require.NoError(t, consumerA.Ack(ctx, gotA[0].ID, 0))
	gotB, err = consumerB.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotB, 1)
	require.NoError(t, consumerB.Ack(ctx, gotB[0].ID, 0))
}

func TestConcurrency_PoolInherits(t *testing.T) {
	pool := NewConsumerPool(nil, "q")
	pool.SetConcurrencyTags(true)
	assert.True(t, pool.concurrencyTags)
}
