package mongo

import (
	"context"
	"os"
	"testing"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/Rinsvent/smart-redis-queue/journal"
	"github.com/stretchr/testify/require"
)

func TestMongo_OffsetOrder(t *testing.T) {
	uri := os.Getenv("SRQ_MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	ctx := context.Background()
	src, err := New(ctx, Config{URI: uri, Database: "srq_test", Collection: "srq_journal_tasks_test"})
	if err != nil {
		t.Skipf("mongo: %v", err)
	}
	defer src.Close(ctx)
	if err := src.Init(ctx); err != nil {
		t.Skipf("mongo init: %v", err)
	}
	_, _ = src.col.DeleteMany(ctx, map[string]any{})
	_, _ = src.ctr.DeleteMany(ctx, map[string]any{})

	require.NoError(t, src.Insert(ctx,
		&redisqueue.Task{ID: "m1", Payload: []byte("a"), Scheduled: time.Now()},
		&redisqueue.Task{ID: "m2", Payload: []byte("b"), Scheduled: time.Now()},
	))
	recs, err := src.ReadBatch(ctx, journal.Cursor{Offset: -1}, 10)
	require.NoError(t, err)
	require.Len(t, recs, 2)
	require.Equal(t, int64(1), recs[0].Cursor.Offset)
	by, err := src.ReadByOffset(ctx, journal.OffsetCursor(1))
	require.NoError(t, err)
	require.Equal(t, "m1", by.Task.ID)
}
