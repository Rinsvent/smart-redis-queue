package mongo

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/Rinsvent/smart-redis-queue/journal"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func testMongo(t *testing.T) (*Source, context.Context) {
	t.Helper()
	uri := os.Getenv("SRQ_MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	ctx := context.Background()
	src, err := New(ctx, Config{URI: uri, Database: "srq_test", Collection: "srq_journal_tasks_v2"})
	if err != nil {
		t.Skipf("mongo: %v", err)
	}
	t.Cleanup(func() { _ = src.Close(ctx) })
	if err := src.Init(ctx); err != nil {
		t.Skipf("mongo init: %v", err)
	}
	_, _ = src.col.DeleteMany(ctx, map[string]any{})
	_, _ = src.ctr.DeleteMany(ctx, map[string]any{})
	return src, ctx
}

func TestMongo_BatchIncReadyOrder(t *testing.T) {
	src, ctx := testMongo(t)
	require.NoError(t, src.Insert(ctx,
		&redisqueue.Task{ID: "m1", Payload: []byte("a"), Scheduled: time.Now()},
		&redisqueue.Task{ID: "m2", Payload: []byte("b"), Scheduled: time.Now()},
	))
	recs, err := src.ReadBatch(ctx, journal.Cursor{Offset: -1}, 10)
	require.NoError(t, err)
	require.Len(t, recs, 2)
	require.Equal(t, int64(1), recs[0].Cursor.Offset)
	require.Equal(t, int64(2), recs[1].Cursor.Offset)
	by, err := src.ReadByOffset(ctx, journal.OffsetCursor(1))
	require.NoError(t, err)
	require.Equal(t, "m1", by.Task.ID)
}

func TestMongo_ScheduledPromoteNoCursorHole(t *testing.T) {
	src, ctx := testMongo(t)
	now := time.Now()
	require.NoError(t, src.Insert(ctx,
		&redisqueue.Task{ID: "far", Payload: []byte("f"), Scheduled: now.Add(time.Hour)},
		&redisqueue.Task{ID: "due", Payload: []byte("d"), Scheduled: now},
	))
	// far не в ready-stream
	recs, err := src.ReadBatch(ctx, journal.Cursor{Offset: -1}, 10)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	require.Equal(t, "due", recs[0].Task.ID)
	require.Equal(t, int64(1), recs[0].Cursor.Offset)

	// simulate due: rewind scheduled into window
	_, err = src.col.UpdateOne(ctx, map[string]any{"task_id": "far"},
		map[string]any{"$set": map[string]any{"scheduled": now.Add(-time.Second)}})
	require.NoError(t, err)
	n, err := src.PromoteDue(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	recs, err = src.ReadBatch(ctx, journal.OffsetCursor(1), 10)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	require.Equal(t, "far", recs[0].Task.ID)
	require.Equal(t, int64(2), recs[0].Cursor.Offset) // новый offset, не дыра
}

func TestMongo_RecoverAfterRedisNoAckedDup(t *testing.T) {
	src, ctx := testMongo(t)
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 15})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis: %v", err)
	}
	defer rdb.Close()
	require.NoError(t, rdb.FlushDB(ctx).Err())

	const n = 10
	now := time.Now()
	tasks := make([]*redisqueue.Task, n)
	for i := 0; i < n; i++ {
		tasks[i] = &redisqueue.Task{ID: fmt.Sprintf("mo-r-%02d", i), Payload: []byte("x"), Scheduled: now}
	}
	require.NoError(t, src.Insert(ctx, tasks...))

	const queue = "mongo-recover-q"
	br := journal.NewBridge(rdb, "mo-rec", queue, src)
	for {
		k, err := br.Pump(ctx, 32)
		require.NoError(t, err)
		if k == 0 {
			break
		}
	}
	c := redisqueue.NewConsumer(rdb, queue, "c1")
	defer c.Close()
	c.SetPrefetchCount(100)
	got, err := c.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, n)

	acked := 4
	for i := 0; i < acked; i++ {
		require.NoError(t, src.Ack(ctx, got[i].ID, nil))
		require.NoError(t, c.Ack(ctx, got[i].ID, 0))
	}

	require.NoError(t, rdb.FlushDB(ctx).Err())
	require.NoError(t, br.RecoverAfterRedis(ctx, nil))

	c2 := redisqueue.NewConsumer(rdb, queue, "c2")
	defer c2.Close()
	c2.SetPrefetchCount(100)
	again, err := c2.Get(ctx)
	require.NoError(t, err)
	require.Len(t, again, n-acked)
	seen := map[string]struct{}{}
	for _, task := range again {
		seen[task.ID] = struct{}{}
	}
	for i := 0; i < acked; i++ {
		_, dup := seen[got[i].ID]
		require.False(t, dup, "acked %s must not replay", got[i].ID)
	}
}
