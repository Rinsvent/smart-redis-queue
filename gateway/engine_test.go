package gateway

import (
	"context"
	"testing"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGateway_PublishGetAck(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 7})
	ctx := context.Background()
	require.NoError(t, rdb.Ping(ctx).Err())
	require.NoError(t, rdb.FlushDB(ctx).Err())
	t.Cleanup(func() { _ = rdb.Close() })

	gw, err := New(Config{
		Redis:              rdb,
		SkipPersistCheck:   true,
		LossWindowOverride: time.Second,
	})
	require.NoError(t, err)
	require.NoError(t, gw.Boot(ctx))
	t.Cleanup(func() { gw.Close() })

	q := "gw-test"
	require.NoError(t, gw.PublishTasks(ctx, q, []*redisqueue.Task{{
		ID:        "a1",
		Partition: "p1",
		Payload:   []byte("hi"),
		Scheduled: time.Now().Add(-time.Second),
	}}, nil))

	tasks, err := gw.GetTasks(ctx, q, "c1", 5)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "a1", tasks[0].ID)

	require.NoError(t, gw.AckTask(ctx, q, "c1", "a1", nil))
	assert.Equal(t, StateRunning, gw.State())
}

func TestGateway_FailClosedOnRedisDownState(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 7})
	ctx := context.Background()
	require.NoError(t, rdb.Ping(ctx).Err())

	gw, err := New(Config{Redis: rdb, SkipPersistCheck: true, LossWindowOverride: time.Second})
	require.NoError(t, err)
	require.NoError(t, gw.Boot(ctx))
	t.Cleanup(func() { gw.Close() })

	gw.state.set(StateRedisDown)
	err = gw.PublishTasks(ctx, "q", []*redisqueue.Task{{ID: "x", Payload: []byte("1")}}, nil)
	require.ErrorIs(t, err, ErrRedisDown)

	_, err = gw.GetTasks(ctx, "q", "c", 1)
	require.ErrorIs(t, err, ErrRedisDown)

	// Ack при RedisDown принимается в op-log
	require.NoError(t, gw.AckTask(ctx, "q", "c", "x", nil))
	assert.GreaterOrEqual(t, gw.ops.len(), 1)
}
