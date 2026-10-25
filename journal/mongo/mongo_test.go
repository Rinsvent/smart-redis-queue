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

func TestMongo_ObjectIDOrderAndBatchInsert(t *testing.T) {
	uri := os.Getenv("SRQ_MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	ctx := context.Background()
	src, err := New(ctx, Config{URI: uri, Database: "srq_test", Collection: "srq_journal_tasks_oid"})
	if err != nil {
		t.Skipf("mongo: %v", err)
	}
	defer src.Close(ctx)
	if err := src.Init(ctx); err != nil {
		t.Skipf("mongo init: %v", err)
	}
	_, _ = src.col.DeleteMany(ctx, map[string]any{})

	require.NoError(t, src.Insert(ctx,
		&redisqueue.Task{ID: "m1", Payload: []byte("a"), Scheduled: time.Now()},
		&redisqueue.Task{ID: "m2", Payload: []byte("b"), Scheduled: time.Now()},
	))
	recs, err := src.ReadBatch(ctx, journal.Cursor{}, 10)
	require.NoError(t, err)
	require.Len(t, recs, 2)
	require.Equal(t, "m1", recs[0].Task.ID)
	require.Equal(t, "m2", recs[1].Task.ID)
	require.NotEmpty(t, recs[0].Cursor.Key)
	require.Len(t, recs[0].Cursor.Key, 24)
	require.True(t, recs[1].Cursor.After(recs[0].Cursor))

	by, err := src.ReadByOffset(ctx, recs[0].Cursor)
	require.NoError(t, err)
	require.Equal(t, "m1", by.Task.ID)

	// продолжение после курсора
	rest, err := src.ReadBatch(ctx, recs[0].Cursor, 10)
	require.NoError(t, err)
	require.Len(t, rest, 1)
	require.Equal(t, "m2", rest[0].Task.ID)
}
