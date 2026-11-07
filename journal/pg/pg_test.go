package pg

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

func testPG(t *testing.T) (*Source, context.Context) {
	t.Helper()
	dsn := os.Getenv("SRQ_PG_DSN")
	if dsn == "" {
		dsn = "postgres://srq:srq@localhost:5432/srq?sslmode=disable"
	}
	ctx := context.Background()
	src, err := New(ctx, Config{DSN: dsn, Table: "srq_journal_tasks_v2"})
	if err != nil {
		t.Skipf("pg: %v", err)
	}
	t.Cleanup(src.Close)
	if err := src.Init(ctx); err != nil {
		t.Skipf("pg init: %v", err)
	}
	_, _ = src.pool.Exec(ctx, `TRUNCATE srq_journal_tasks_v2 RESTART IDENTITY`)
	_, _ = src.pool.Exec(ctx, `ALTER SEQUENCE srq_journal_tasks_v2_offset_seq RESTART WITH 1`)
	return src, ctx
}

func TestPG_OffsetOrderAndAck(t *testing.T) {
	src, ctx := testPG(t)
	ts := time.Now().UTC()
	require.NoError(t, src.Insert(ctx,
		&redisqueue.Task{ID: "id-1", Payload: []byte("a"), Tags: []string{"x"}, Groups: []string{"g"}, Scheduled: ts},
		&redisqueue.Task{ID: "id-2", Payload: []byte("b"), Tags: []string{"x"}, Groups: []string{"g"}, Scheduled: ts},
	))
	recs, err := src.ReadBatch(ctx, journal.Cursor{}, 10)
	require.NoError(t, err)
	require.Len(t, recs, 2)
	require.Equal(t, "id-1", recs[0].Task.ID)
	require.Equal(t, int64(1), recs[0].Cursor.Offset)
	require.Equal(t, int64(2), recs[1].Cursor.Offset)

	byOff, err := src.ReadByOffset(ctx, journal.OffsetCursor(2))
	require.NoError(t, err)
	require.Equal(t, "id-2", byOff.Task.ID)

	require.NoError(t, src.Ack(ctx, "id-1", nil))
	recs, err = src.ReadBatch(ctx, journal.Cursor{}, 10)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	require.Equal(t, "id-2", recs[0].Task.ID)
}

func TestPG_ScheduledPromote(t *testing.T) {
	src, ctx := testPG(t)
	now := time.Now().UTC()
	require.NoError(t, src.Insert(ctx,
		&redisqueue.Task{ID: "far", Payload: []byte("f"), Scheduled: now.Add(time.Hour)},
		&redisqueue.Task{ID: "due", Payload: []byte("d"), Scheduled: now},
	))
	recs, err := src.ReadBatch(ctx, journal.Cursor{}, 10)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	require.Equal(t, "due", recs[0].Task.ID)

	_, err = src.pool.Exec(ctx, `UPDATE srq_journal_tasks_v2 SET scheduled = $1 WHERE task_id = 'far'`, now.Add(-time.Second))
	require.NoError(t, err)
	n, err := src.PromoteDue(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	recs, err = src.ReadBatch(ctx, journal.OffsetCursor(1), 10)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	require.Equal(t, "far", recs[0].Task.ID)
	require.Equal(t, int64(2), recs[0].Cursor.Offset)
}

func TestPG_RecoverAfterRedisNoAckedDup(t *testing.T) {
	src, ctx := testPG(t)
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 14})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis: %v", err)
	}
	defer rdb.Close()
	require.NoError(t, rdb.FlushDB(ctx).Err())

	const n = 10
	now := time.Now().UTC()
	tasks := make([]*redisqueue.Task, n)
	for i := 0; i < n; i++ {
		tasks[i] = &redisqueue.Task{ID: fmt.Sprintf("pg-r-%02d", i), Payload: []byte("x"), Scheduled: now}
	}
	require.NoError(t, src.Insert(ctx, tasks...))

	const queue = "pg-recover-q"
	br := journal.NewBridge(rdb, "pg-rec", queue, src)
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
		require.NoError(t, c.Ack(ctx, got[i].ID))
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
