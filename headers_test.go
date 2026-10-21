package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeaders_SetGetAckClears(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	ctx := context.Background()
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "h1", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	tasks, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	task := tasks[0]

	m, err := task.Headers(ctx)
	require.NoError(t, err)
	assert.Empty(t, m)

	require.NoError(t, task.SetHeader(ctx, "last_error", "timeout"))
	require.NoError(t, task.SetHeader(ctx, "attempt", "2"))
	m, err = task.Headers(ctx)
	require.NoError(t, err)
	assert.Equal(t, "timeout", m["last_error"])
	assert.Equal(t, "2", m["attempt"])

	require.NoError(t, consumer.Reject(ctx, task.ID, 0))

	tasks2, err := consumer.Get(ctx)
	require.NoError(t, err)
	m2, err := tasks2[0].Headers(ctx)
	require.NoError(t, err)
	assert.Equal(t, "timeout", m2["last_error"], "headers survive Reject")

	require.NoError(t, tasks2[0].SetHeaders(ctx, map[string]string{"last_error": "ok-retry"}))
	require.NoError(t, consumer.Ack(ctx, tasks2[0].ID, 0))
	assert.Equal(t, int64(0), rdb.Exists(ctx, headersKey("test-queue", "h1")).Val())
}

func TestHeaders_RequiresBind(t *testing.T) {
	ctx := context.Background()
	_, err := (&Task{ID: "x"}).Headers(ctx)
	require.Error(t, err)
}
