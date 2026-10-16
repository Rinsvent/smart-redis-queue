package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimit_SetRateLimit_Validate(t *testing.T) {
	_, consumer, _ := setupTestQueue(t)
	require.Error(t, consumer.SetRateLimit(&RateLimitConfig{
		Algorithm: "leaky-bucket",
		Group:     &RateLimitLayer{Rates: []Rate{{time.Second, 1}}},
	}))
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Group: &RateLimitLayer{Rates: []Rate{{time.Second, 10}}},
	}))
	require.NoError(t, consumer.SetRateLimit(nil))
}

func TestRateLimit_GroupLayer_BlocksGroup(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Group: &RateLimitLayer{Rates: []Rate{{time.Hour, 1}}},
	}))
	ctx := context.Background()
	q := "test-queue"

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "a1", Partition: "!tok:chat-1", Groups: []string{"token:1"},
			Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "a2", Partition: "!tok:chat-2", Groups: []string{"token:1"},
			Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second)},
	))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2, "group limit 1/h — вторая партиция того же token не выдаётся")

	score, err := rdb.ZScore(ctx, "queue:"+q+":groups", "token:1").Result()
	require.NoError(t, err)
	assert.Greater(t, score, float64(time.Now().UnixMilli()))

	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestRateLimit_PartitionLayer_BlocksPartition(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Partition: &RateLimitLayer{Rates: []Rate{{time.Hour, 1}}},
	}))
	ctx := context.Background()
	q := "test-queue"
	part := "user-421"

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "p1", Partition: part, Payload: []byte("1"), Scheduled: time.Now().Add(-time.Second),
	}))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "p2", Partition: part, Payload: []byte("2"), Scheduled: time.Now().Add(-time.Second),
	}))

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)

	score, err := rdb.ZScore(ctx, "queue:"+q+":blocked", part).Result()
	require.NoError(t, err)
	assert.Greater(t, score, float64(time.Now().UnixMilli()))
}

func TestRateLimit_BothLayers_GroupFirst(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Group:     &RateLimitLayer{Rates: []Rate{{time.Hour, 1}}},
		Partition: &RateLimitLayer{Rates: []Rate{{time.Hour, 100}}},
	}))
	ctx := context.Background()
	q := "test-queue"

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "b1", Partition: "!t:c1", Groups: []string{"tok"},
			Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "b2", Partition: "!t:c2", Groups: []string{"tok"},
			Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second)},
	))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)

	// упёрлись в group — blocked partition для c2 может не появиться (counter partition не жгли)
	gScore, err := rdb.ZScore(ctx, "queue:"+q+":groups", "tok").Result()
	require.NoError(t, err)
	assert.Greater(t, gScore, float64(time.Now().UnixMilli()))

	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestRateLimit_Disabled_NoExtraKeys(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "n1", Partition: "p", Groups: []string{"g"},
		Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	var cursor uint64
	var rlKeys []string
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "queue:test-queue:rl:*", 100).Result()
		require.NoError(t, err)
		rlKeys = append(rlKeys, keys...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	assert.Empty(t, rlKeys)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestRateLimit_PoolInherits(t *testing.T) {
	pool := NewConsumerPool(nil, "q")
	require.NoError(t, pool.SetRateLimit(&RateLimitConfig{
		Group: &RateLimitLayer{Rates: []Rate{{time.Second, 5}}},
	}))
	require.NotNil(t, pool.rateLimit)
	require.NoError(t, pool.SetRateLimit(nil))
	assert.Nil(t, pool.rateLimit)
}
