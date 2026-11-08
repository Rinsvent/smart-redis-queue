package gateway

import (
	"context"
	"fmt"
	"testing"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gatewayTestDB — отдельная Redis DB (не пересекается с root/consumer_opts DB7 и bench DB5).
const gatewayTestDB = 4

func gatewayRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: gatewayTestDB})
	ctx := context.Background()
	require.NoError(t, rdb.Ping(ctx).Err())
	require.NoError(t, rdb.FlushDB(ctx).Err())
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestGateway_PublishGetAck(t *testing.T) {
	rdb := gatewayRedis(t)
	ctx := context.Background()

	gw, err := New(Config{
		Redis:              rdb,
		SkipPersistCheck:   true,
		LossWindowOverride: time.Second,
	})
	require.NoError(t, err)
	require.NoError(t, gw.Boot(ctx))
	t.Cleanup(func() { gw.Close() })

	q := fmt.Sprintf("gw-test-%d", time.Now().UnixNano())
	cid := "c1"
	tid := "a1"
	require.NoError(t, gw.PublishTasks(ctx, q, []*redisqueue.Task{{
		ID:        tid,
		Partition: "p1",
		Payload:   []byte("hi"),
		Scheduled: time.Now().Add(-time.Second),
	}}, nil))

	tasks, err := gw.GetTasks(ctx, q, cid, 5)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, tid, tasks[0].ID)

	require.NoError(t, gw.AckTask(ctx, q, cid, tid, nil))
	assert.Equal(t, StateRunning, gw.State())
}

func TestGateway_FailClosedOnRedisDownState(t *testing.T) {
	rdb := gatewayRedis(t)
	ctx := context.Background()

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

func TestGateway_RecoverMarksPendingOK(t *testing.T) {
	rdb := gatewayRedis(t)
	ctx := context.Background()

	gw, err := New(Config{Redis: rdb, SkipPersistCheck: true, LossWindowOverride: time.Second})
	require.NoError(t, err)
	require.NoError(t, gw.Boot(ctx))
	t.Cleanup(func() { gw.Close() })

	q := fmt.Sprintf("gw-rec-%d", time.Now().UnixNano())
	require.NoError(t, gw.PublishTasks(ctx, q, []*redisqueue.Task{{
		ID: "r1", Partition: "p", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}}, nil))
	tasks, err := gw.GetTasks(ctx, q, "c-rec", 1)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	gw.state.set(StateRedisDown)
	require.NoError(t, gw.AckTask(ctx, q, "c-rec", "r1", nil))
	pending := 0
	for _, op := range gw.ops.snapshot() {
		if !op.RedisOK {
			pending++
		}
	}
	require.Equal(t, 1, pending)

	require.NoError(t, gw.recover(ctx))
	assert.Equal(t, StateRunning, gw.State())
	for _, op := range gw.ops.snapshot() {
		assert.True(t, op.RedisOK, "offset %d still pending", op.Offset)
	}
	// повторный recover не должен падать на уже settled Ack
	gw.state.set(StateRedisDown)
	gw.ops.append(Op{Kind: OpAck, Queue: q, Consumer: "c-rec", TaskID: "r1", RedisOK: false})
	require.NoError(t, gw.recover(ctx))
	assert.Equal(t, StateRunning, gw.State())
}
