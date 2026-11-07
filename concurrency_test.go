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
	assert.True(t, HasRaceTagPrefix(RaceSharedTag("file:abc")))
	assert.True(t, IsRaceSharedTag(RaceSharedTag("file:abc")))
	assert.False(t, IsRaceSharedTag(RaceTagPrefix+"file:abc"))
	id, ok := RaceSharedID(RaceSharedTag("file:abc"))
	assert.True(t, ok)
	assert.Equal(t, "file:abc", id)
	assert.True(t, IsRaceBlockTag(RaceBlockTag("batch:1")))
	assert.Equal(t, "batch:1", NormalizeRaceBlockID(RaceBlockTag("batch:1")))
	assert.Equal(t, "batch:1", NormalizeRaceBlockID("batch:1"))
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
	require.NoError(t, consumer.Ack(ctx, got1[0].ID))
	require.NoError(t, consumer.Ack(ctx, got2[0].ID))
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

	require.NoError(t, consumerA.Ack(ctx, gotA[0].ID))
	assert.False(t, rdb.HExists(ctx, "queue:test-queue:race:locks", tag).Val())

	gotB, err = consumerB.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotB, 1)
	require.NoError(t, consumerB.Ack(ctx, gotB[0].ID))
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

	require.NoError(t, consumer.Reject(ctx, got[0].ID))
	assert.False(t, rdb.HExists(ctx, "queue:test-queue:race:locks", tag).Val())
	assert.Equal(t, int64(1), rdb.Exists(ctx, "queue:test-queue:race:tags:r1").Val())

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	require.NoError(t, consumer.Ack(ctx, got2[0].ID))
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

	require.NoError(t, consumerA.Ack(ctx, gotA[0].ID))
	gotB, err = consumerB.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotB, 1)
	require.NoError(t, consumerB.Ack(ctx, gotB[0].ID))
}

func TestConcurrency_PoolInherits(t *testing.T) {
	pool := NewConsumerPool(nil, "q")
	pool.SetConcurrencyTags(true)
	assert.True(t, pool.concurrencyTags)
}

func TestConcurrency_RaceShared_AckUnlocksParallel(t *testing.T) {
	producer, consumerA, rdb := setupTestQueue(t)
	consumerB := NewConsumer(consumerA.redis, "test-queue", "sb")
	t.Cleanup(func() { consumerB.Close() })
	consumerA.SetPrefetchCount(1)
	consumerB.SetPrefetchCount(1)
	consumerA.SetConcurrencyTags(true)
	consumerB.SetConcurrencyTags(true)
	ctx := context.Background()
	sharedID := "file:mail1"
	tag := RaceSharedTag(sharedID)
	blob := []byte(`{"tg":"token-1"}`)

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "s1", Partition: "p1", Payload: []byte("1"), Tags: []string{tag},
			Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "s2", Partition: "p2", Payload: []byte("2"), Tags: []string{tag},
			Scheduled: time.Now().Add(-time.Second)},
	))

	gotA, err := consumerA.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotA, 1)
	assert.Empty(t, gotA[0].Shared, "лидер без Shared")
	assert.True(t, rdb.HExists(ctx, "queue:test-queue:race:locks", tag).Val())

	gotB, err := consumerB.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, gotB, "ждём prepare")

	require.NoError(t, consumerA.Ack(ctx, gotA[0].ID,
		WithShared(map[string][]byte{sharedID: blob}),
		WithSharedTTL(time.Hour),
	))
	assert.False(t, rdb.HExists(ctx, "queue:test-queue:race:locks", tag).Val())
	assert.Equal(t, blob, []byte(rdb.Get(ctx, "queue:test-queue:shared:"+sharedID).Val()))

	gotB, err = consumerB.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotB, 1)
	require.Equal(t, blob, gotB[0].Shared[sharedID])
	require.NoError(t, consumerB.Ack(ctx, gotB[0].ID))
}

func TestConcurrency_RaceShared_ShareMidHandler(t *testing.T) {
	producer, consumerA, rdb := setupTestQueue(t)
	consumerB := NewConsumer(consumerA.redis, "test-queue", "sm")
	t.Cleanup(func() { consumerB.Close() })
	consumerA.SetPrefetchCount(1)
	consumerB.SetPrefetchCount(1)
	consumerA.SetConcurrencyTags(true)
	consumerB.SetConcurrencyTags(true)
	ctx := context.Background()
	sharedID := "file:mid"
	tag := RaceSharedTag(sharedID)
	blob := []byte("TOK")

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "m1", Partition: "a", Payload: []byte("1"), Tags: []string{tag},
			Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "m2", Partition: "b", Payload: []byte("2"), Tags: []string{tag},
			Scheduled: time.Now().Add(-time.Second)},
	))

	gotA, err := consumerA.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotA, 1)

	sh, err := NewShared(ctx, gotA[0])
	require.NoError(t, err)
	require.NoError(t, sh.Publish(ctx, map[string][]byte{sharedID: blob}))
	assert.False(t, rdb.HExists(ctx, "queue:test-queue:race:locks", tag).Val())
	assert.Equal(t, blob, gotA[0].Shared[sharedID])

	gotB, err := consumerB.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotB, 1, "после Share параллельно")
	assert.Equal(t, blob, gotB[0].Shared[sharedID])

	// overwrite
	blob2 := []byte("TOK2")
	require.NoError(t, sh.Publish(ctx, map[string][]byte{sharedID: blob2}))
	assert.Equal(t, string(blob2), rdb.Get(ctx, "queue:test-queue:shared:"+sharedID).Val())

	require.NoError(t, consumerA.Ack(ctx, gotA[0].ID))
	require.NoError(t, consumerB.Ack(ctx, gotB[0].ID))
}

func TestConcurrency_RaceShared_RejectAllowsNewLeader(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	consumer.SetConcurrencyTags(true)
	ctx := context.Background()
	tag := RaceSharedTag("file:rej2")

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "r1", Partition: "p", Payload: []byte("x"), Tags: []string{tag},
		Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Reject(ctx, got[0].ID))
	assert.False(t, rdb.HExists(ctx, "queue:test-queue:race:locks", tag).Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:test-queue:shared:file:rej2").Val())

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	assert.Empty(t, got2[0].Shared)
	require.NoError(t, consumer.Ack(ctx, got2[0].ID,
		WithShared(map[string][]byte{"file:rej2": []byte("ok")}),
	))
}

func TestConcurrency_RaceShared_LatePublishGetsBlob(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	consumer.SetConcurrencyTags(true)
	ctx := context.Background()
	sharedID := "file:late"
	tag := RaceSharedTag(sharedID)
	blob := []byte("READY")

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "early", Partition: "p1", Payload: []byte("1"), Tags: []string{tag},
		Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, WithShared(map[string][]byte{sharedID: blob})))

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "late", Partition: "p2", Payload: []byte("2"), Tags: []string{tag},
		Scheduled: time.Now().Add(-time.Second),
	}))
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	assert.Equal(t, "late", got2[0].ID)
	assert.Equal(t, blob, got2[0].Shared[sharedID])
	require.NoError(t, consumer.Ack(ctx, got2[0].ID))
}

func TestConcurrency_RaceBlock_ParkProbeOpen(t *testing.T) {
	producer, consumerA, rdb := setupTestQueue(t)
	consumerB := NewConsumer(consumerA.redis, "test-queue", "bb")
	t.Cleanup(func() { consumerB.Close() })
	consumerA.SetPrefetchCount(1)
	consumerB.SetPrefetchCount(1)
	consumerA.SetConcurrencyTags(true)
	consumerB.SetConcurrencyTags(true)
	ctx := context.Background()

	sharedID := "file:b1"
	blockID := "batch:b1"
	tags := []string{RaceSharedTag(sharedID), RaceBlockTag(blockID)}
	blob := []byte("TOK")

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "b1", Partition: "p1", Payload: []byte("1"), Tags: tags,
			Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "b2", Partition: "p2", Payload: []byte("2"), Tags: tags,
			Scheduled: time.Now().Add(-time.Second)},
	))

	gotA, err := consumerA.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotA, 1)

	sh, err := NewShared(ctx, gotA[0])
	require.NoError(t, err)
	require.NoError(t, sh.Publish(ctx, map[string][]byte{sharedID: blob}))

	// not ready → park
	require.NoError(t, consumerA.Reject(ctx, gotA[0].ID, WithDelay(2), WithBlockTags(blockID)))
	assert.Equal(t, BlockWaitValue, rdb.Get(ctx, "queue:test-queue:block:"+blockID).Val())

	gotB, err := consumerB.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, gotB, "block wait → skip")

	require.Eventually(t, func() bool {
		return rdb.Exists(ctx, "queue:test-queue:block:"+blockID).Val() == 0
	}, 5*time.Second, 50*time.Millisecond)

	gotB, err = consumerB.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotB, 1, "после TTL один probe")
	assert.Equal(t, blob, gotB[0].Shared[sharedID])

	shB, err := NewShared(ctx, gotB[0])
	require.NoError(t, err)
	require.NoError(t, shB.Open(ctx, []string{blockID}, WithSharedTTL(time.Hour)))
	assert.Equal(t, BlockReadyValue, rdb.Get(ctx, "queue:test-queue:block:"+blockID).Val())
	require.NoError(t, consumerB.Ack(ctx, gotB[0].ID))

	gotA2, err := consumerA.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotA2, 1, "ready → parallel без лока")
	assert.Equal(t, blob, gotA2[0].Shared[sharedID])
	require.NoError(t, consumerA.Ack(ctx, gotA2[0].ID))
}
