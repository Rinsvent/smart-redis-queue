package redisqueue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublish_Backpressure_WithTagBackpressure(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	tag := "bp-stream"

	for i := 0; i < 3; i++ {
		require.NoError(t, producer.Publish(ctx, &Task{
			ID: fmt.Sprintf("a-%d", i), Tags: []string{tag},
			Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
		}))
	}
	assert.Equal(t, int64(3), rdb.SCard(ctx, "queue:test-queue:tag:"+tag).Val())

	err := producer.PublishWith(ctx, []PublishOption{
		WithTagBackpressure(tag, 3),
	}, &Task{
		ID: "blocked", Tags: []string{tag},
		Payload: []byte("y"), Scheduled: time.Now().Add(-time.Second),
	})
	var bp *ErrBackpressure
	require.ErrorAs(t, err, &bp)
	assert.Equal(t, tag, bp.Tag)
	assert.Equal(t, int64(3), bp.Count)
	assert.Equal(t, int64(3), bp.Limit)
	assert.Equal(t, int64(0), rdb.Exists(ctx, payloadKey("test-queue", "blocked")).Val())

	// после Ack место освобождается
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	require.NoError(t, producer.PublishWith(ctx, []PublishOption{
		WithTagBackpressure(tag, 3),
	}, &Task{
		ID: "ok-after-ack", Tags: []string{tag},
		Payload: []byte("z"), Scheduled: time.Now().Add(-time.Second),
	}))
}

func TestPublish_Backpressure_WithTagLimits(t *testing.T) {
	producer, _, rdb := setupTestQueue(t)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "tg-1", Tags: []string{"tg"}, Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "vk-1", Tags: []string{"vk:123"}, Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))

	err := producer.PublishWith(ctx, []PublishOption{
		WithTagLimits(map[string]int64{"tg": 1, "vk:123": 10}),
	}, &Task{
		ID: "more-tg", Tags: []string{"tg"}, Payload: []byte("y"), Scheduled: time.Now().Add(-time.Second),
	})
	var bp *ErrBackpressure
	require.ErrorAs(t, err, &bp)
	assert.Equal(t, "tg", bp.Tag)

	// AND по всем лимитам вызова: при полном tg нельзя публиковать даже в vk —
	// для потока vk передаём только его квоту (и опционально другие свободные).
	require.NoError(t, producer.PublishWith(ctx, []PublishOption{
		WithTagLimits(map[string]int64{"vk:123": 10}),
		WithTagBackpressure("extra", 100),
	}, &Task{
		ID: "vk-2", Tags: []string{"vk:123"}, Payload: []byte("y"), Scheduled: time.Now().Add(-time.Second),
	}))
	assert.Equal(t, int64(2), rdb.SCard(ctx, "queue:test-queue:tag:vk:123").Val())
}

func TestPublish_Backpressure_BatchRejectedAtomically(t *testing.T) {
	producer, _, rdb := setupTestQueue(t)
	ctx := context.Background()
	tag := "batch-bp"

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "one", Tags: []string{tag}, Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))

	err := producer.PublishWith(ctx, []PublishOption{
		WithTagBackpressure(tag, 2),
	},
		&Task{ID: "b1", Tags: []string{tag}, Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "b2", Tags: []string{tag}, Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second)},
	)
	var bp *ErrBackpressure
	require.ErrorAs(t, err, &bp)
	assert.Equal(t, int64(0), rdb.Exists(ctx, payloadKey("test-queue", "b1")).Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, payloadKey("test-queue", "b2")).Val())
	assert.Equal(t, int64(1), rdb.SCard(ctx, "queue:test-queue:tag:"+tag).Val())
}

func TestPublish_Backpressure_NoOptsUnchanged(t *testing.T) {
	producer, _, _ := setupTestQueue(t)
	ctx := context.Background()
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "plain", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	err := producer.Publish(ctx, &Task{
		ID: "plain", Payload: []byte("dup"), Scheduled: time.Now().Add(-time.Second),
	})
	var exists *ErrTasksAlreadyExist
	require.True(t, errors.As(err, &exists))
}

func TestPublish_Backpressure_InvalidTag(t *testing.T) {
	producer, _, _ := setupTestQueue(t)
	err := producer.PublishWith(context.Background(), []PublishOption{
		WithTagBackpressure("bad"+TagSeparator+"tag", 10),
	}, &Task{ID: "x", Payload: []byte("x")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "forbidden separator")
}
