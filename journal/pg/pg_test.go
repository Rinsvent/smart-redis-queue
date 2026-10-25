package pg

import (
	"context"
	"os"
	"testing"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/Rinsvent/smart-redis-queue/journal"
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
