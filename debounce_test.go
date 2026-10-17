package redisqueue

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDebounceID_Helpers(t *testing.T) {
	assert.True(t, HasDebouncePrefix(DebouncePrefix+"job"))
	assert.False(t, HasDebouncePrefix("job"))
	assert.Equal(t, DebouncePrefix+"job", DebounceID("job"))
	assert.Equal(t, DebouncePrefix+"job", DebounceID(DebouncePrefix+"job"))
}

func TestDebounce_Waiting_OverridePayloadAndScore(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	id := DebounceID("stats-day")
	until := time.Now().Add(2 * time.Second)

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: id, Payload: []byte("v1"), Scheduled: until,
	}))
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: id, Payload: []byte("v2"), Scheduled: until,
	}))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got, "до until Get пустой")

	raw, err := rdb.Get(ctx, "queue:test-queue:payload:"+id).Bytes()
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), raw)

	score, err := rdb.ZScore(ctx, "queue:test-queue:partition:base:0", id).Result()
	require.NoError(t, err)
	assert.InDelta(t, float64(until.UnixMilli()), score, 5)

	// после until — выдаём последний payload
	require.Eventually(t, func() bool {
		got, err = consumer.Get(ctx)
		return err == nil && len(got) == 1
	}, 3*time.Second, 50*time.Millisecond)
	require.Equal(t, id, got[0].ID)
	assert.Equal(t, []byte("v2"), got[0].Payload)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestDebounce_InFlight_AckReactivates(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	id := DebounceID("job-1")

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: id, Payload: []byte("old"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []byte("old"), got[0].Payload)

	until := time.Now().Add(500 * time.Millisecond)
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: id, Payload: []byte("mid"), Scheduled: until,
	}))
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: id, Payload: []byte("last"), Scheduled: until,
	}))

	flag, err := rdb.Get(ctx, "queue:test-queue:debounce:"+id).Result()
	require.NoError(t, err)
	flagMs, err := strconv.ParseInt(flag, 10, 64)
	require.NoError(t, err)
	assert.Equal(t, until.UnixMilli(), flagMs)

	// пока in-flight — второго Get нет
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)

	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	// флаг снят, задача снова в ZSET
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:test-queue:debounce:"+id).Val())
	score, err := rdb.ZScore(ctx, "queue:test-queue:partition:base:0", id).Result()
	require.NoError(t, err)
	assert.InDelta(t, float64(until.UnixMilli()), score, 5)

	require.Eventually(t, func() bool {
		got2, err = consumer.Get(ctx)
		return err == nil && len(got2) == 1
	}, 2*time.Second, 50*time.Millisecond)
	assert.Equal(t, []byte("last"), got2[0].Payload)
	require.NoError(t, consumer.Ack(ctx, got2[0].ID, 0))
}

func TestDebounce_InFlight_AckWithIdempotencyTtl_NoMarker(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	id := DebounceID("idem")

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: id, Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: id, Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second),
	}))
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 30*time.Second))

	// reactivate: payload живой с данными, не F1-маркер "0"
	raw, err := rdb.Get(ctx, "queue:test-queue:payload:"+id).Bytes()
	require.NoError(t, err)
	assert.Equal(t, []byte("b"), raw)
	ttl := rdb.TTL(ctx, "queue:test-queue:payload:"+id).Val()
	assert.True(t, ttl < 0, "не должно быть EXPIRE от F1, ttl=%v", ttl)

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	assert.Equal(t, []byte("b"), got2[0].Payload)
	require.NoError(t, consumer.Ack(ctx, got2[0].ID, 0))
}

func TestDebounce_Reject_ClearsFlagKeepsPayload(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	id := DebounceID("rej")

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: id, Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: id, Payload: []byte("b"), Scheduled: time.Now().Add(time.Hour),
	}))
	assert.Equal(t, int64(1), rdb.Exists(ctx, "queue:test-queue:debounce:"+id).Val())

	require.NoError(t, consumer.Reject(ctx, got[0].ID, 0))
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:test-queue:debounce:"+id).Val())

	raw, err := rdb.Get(ctx, "queue:test-queue:payload:"+id).Bytes()
	require.NoError(t, err)
	assert.Equal(t, []byte("b"), raw)

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	assert.Equal(t, []byte("b"), got2[0].Payload)
	require.NoError(t, consumer.Ack(ctx, got2[0].ID, 0))
}

func TestDebounce_WithoutPrefix_DuplicateNX(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "plain", Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second),
	}))
	err := producer.Publish(ctx, &Task{
		ID: "plain", Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second),
	})
	require.Error(t, err)
	var exists *ErrTasksAlreadyExist
	require.ErrorAs(t, err, &exists)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []byte("a"), got[0].Payload)

	var cursor uint64
	var found bool
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "queue:test-queue:debounce:*", 100).Result()
		require.NoError(t, err)
		if len(keys) > 0 {
			found = true
			break
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	assert.False(t, found)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}
