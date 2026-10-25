package journal

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

func testRedis(t testing.TB) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 6})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis: %v", err)
	}
	require.NoError(t, rdb.FlushDB(context.Background()).Err())
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestBridge_PumpAndAckNoLossAfterFlush(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	src := NewMemSource()
	base := time.Now().Add(-time.Second)
	const n = 50
	for i := 0; i < n; i++ {
		src.Insert(&redisqueue.Task{
			ID:        fmt.Sprintf("01task%04d", i),
			Partition: "p",
			Payload:   []byte("x"),
			Tags:      []string{"t1"},
			Groups:    []string{"g1"},
			Scheduled: base.Add(time.Duration(i) * time.Millisecond),
		})
	}

	br := NewBridge(rdb, "j1", "jq", src)
	for {
		k, err := br.Pump(ctx, 10)
		require.NoError(t, err)
		if k == 0 {
			break
		}
	}

	c := redisqueue.NewConsumer(rdb, "jq", "c1")
	defer c.Close()
	c.SetPrefetchCount(100)
	got, err := c.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, n)

	acked := 30
	for i := 0; i < acked; i++ {
		require.NoError(t, src.Ack(ctx, got[i].ID, nil))
		require.NoError(t, c.Ack(ctx, got[i].ID, 0))
	}
	assert.Equal(t, n-acked, src.Pending())

	require.NoError(t, rdb.FlushDB(ctx).Err())

	for {
		k, err := br.Pump(ctx, 20)
		require.NoError(t, err)
		if k == 0 {
			break
		}
	}
	c2 := redisqueue.NewConsumer(rdb, "jq", "c2")
	defer c2.Close()
	c2.SetPrefetchCount(200)
	again, err := c2.Get(ctx)
	require.NoError(t, err)
	require.Len(t, again, n-acked)
	ids := map[string]struct{}{}
	for _, task := range again {
		ids[task.ID] = struct{}{}
	}
	for i := 0; i < acked; i++ {
		_, dup := ids[got[i].ID]
		assert.False(t, dup, "acked %s replayed", got[i].ID)
	}
}

func TestPublishJournal_CursorRewind(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	src := NewMemSource()
	src.Insert(
		&redisqueue.Task{ID: "a-1", Payload: []byte("1"), Scheduled: time.Now()},
		&redisqueue.Task{ID: "b-2", Payload: []byte("2"), Scheduled: time.Now()},
	)

	br := NewBridge(rdb, "jrw", "qrw", src)
	n, err := br.Pump(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	err = redisqueue.PublishJournalBatch(ctx, rdb, "qrw", "jrw", OffsetCursor(1).Encode(),
		[]*redisqueue.Task{{ID: "old", Payload: []byte("x"), Scheduled: time.Now()}})
	require.ErrorIs(t, err, redisqueue.ErrJournalCursorRewind)
}

func TestBridge_RecoverSetsResume(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	src := NewMemSource()
	br := NewBridge(rdb, "jr", "qr", src)
	require.NoError(t, br.RecoverAfterRedis(ctx, nil))
	at, ok, err := redisqueue.GetResumeAfter(ctx, rdb)
	require.NoError(t, err)
	require.True(t, ok)
	assert.False(t, at.IsZero())
}

func TestMem_ReadByOffset(t *testing.T) {
	src := NewMemSource()
	src.Insert(&redisqueue.Task{ID: "x", Payload: []byte("p"), Scheduled: time.Now()})
	rec, err := src.ReadByOffset(context.Background(), OffsetCursor(1))
	require.NoError(t, err)
	assert.Equal(t, "x", rec.Task.ID)
}

func TestPumpLock_Exclusive(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	u1, err := acquirePumpLock(ctx, rdb, "lockj", "h1")
	require.NoError(t, err)
	defer u1()
	_, err = acquirePumpLock(ctx, rdb, "lockj", "h2")
	require.Error(t, err)
}
