package redisqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdmin_DLQ_TagsOffIndex_RetryRestores(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "dlq-tags"

	producer := NewProducer(rdb, q)
	consumer := newConsumer(rdb, q, "", false)
	consumer.SetPrefetchCount(1)

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "t1", Payload: []byte("x"), Tags: []string{"camp"},
		Scheduled: time.Now().Add(-time.Second),
	}))
	n, err := admin.CountByTag(ctx, q, "camp")
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	tasks, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.NoError(t, consumer.Reject(ctx, "t1", WithDeadLetter()))

	n, err = admin.CountByTag(ctx, q, "camp")
	require.NoError(t, err)
	assert.Equal(t, int64(0), n, "DLQ must leave tag index")

	// tags:{id} still for Retry
	members, err := rdb.SMembers(ctx, "queue:"+q+":tags:t1").Result()
	require.NoError(t, err)
	assert.Contains(t, members, "camp")

	_, err = admin.RetryDLQ(ctx, q, "t1")
	require.NoError(t, err)
	n, err = admin.CountByTag(ctx, q, "camp")
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

func TestAdmin_DLQ_RemoveByID(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "dlq-rm"

	producer := NewProducer(rdb, q)
	consumer := newConsumer(rdb, q, "", false)
	consumer.SetPrefetchCount(1)
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "dead", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	tasks, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.NoError(t, consumer.Reject(ctx, "dead", WithDeadLetter()))

	res, err := admin.Remove(ctx, q, "dead")
	require.NoError(t, err)
	assert.Equal(t, RemovalStatusRemoved, res.Status)

	n, err := admin.CountDLQ(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestConsume_OrderedSibling_NoDeadLetterInherit(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "dlq-ord"

	producer := NewProducer(rdb, q)
	consumer := newConsumer(rdb, q, "", false)
	consumer.SetPrefetchCount(2)
	consumer.SetSamePartitionBatch(true)
	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "a", Partition: "!p", Payload: []byte("1"), Scheduled: time.Now().Add(-2 * time.Second)},
		&Task{ID: "b", Partition: "!p", Payload: []byte("2"), Scheduled: time.Now().Add(-time.Second)},
	))

	batch, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 2, "same-partition prefetch must take both")
	require.True(t, isOrderedPartition(batch[0].Partition))

	// как Consume: первая → DeadLetter, остальные ordered siblings → plain Reject
	rejected := map[string]bool{}
	for i, task := range batch {
		if isOrderedPartition(task.Partition) && rejected[task.Partition] {
			require.NoError(t, consumer.Reject(ctx, task.ID))
			continue
		}
		if i == 0 {
			require.NoError(t, consumer.Reject(ctx, task.ID, WithDeadLetter()))
			rejected[task.Partition] = true
			continue
		}
		t.Fatalf("expected sibling %s to take plain-reject branch", task.ID)
	}

	n, err := admin.CountDLQ(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	depth, err := admin.QueueDepth(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(1), depth)
}

func TestAdmin_DLQ_RejectRetryPurge(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "dlq-queue"

	producer := NewProducer(rdb, q)
	consumer := newConsumer(rdb, q, "", false)
	consumer.SetPrefetchCount(1)

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "poison", Payload: []byte("x"), Partition: "p1",
		Scheduled: time.Now().Add(-time.Second),
	}))

	depthBefore, err := admin.QueueDepth(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(1), depthBefore)

	tasks, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	require.NoError(t, consumer.Reject(ctx, "poison", WithDeadLetter()))

	n, err := admin.CountDLQ(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	depthAfter, err := admin.QueueDepth(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(0), depthAfter)

	entries, err := admin.ListDLQ(ctx, q, 0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "poison", entries[0].ID)
	assert.Equal(t, "p1", entries[0].Partition)
	assert.GreaterOrEqual(t, entries[0].RejectCount, int64(1))
	assert.False(t, entries[0].DeadLetterAt.IsZero())

	ti, err := admin.InspectTask(ctx, q, "poison")
	require.NoError(t, err)
	require.True(t, ti.Exists)
	assert.Equal(t, "dead_letter", ti.State)
	assert.Equal(t, "x", ti.Payload)

	// пока в DLQ — Get пустой
	empty, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, empty)

	retried, err := admin.RetryDLQ(ctx, q, "poison")
	require.NoError(t, err)
	assert.Equal(t, 1, retried)

	n, err = admin.CountDLQ(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	depth, err := admin.QueueDepth(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(1), depth)

	tasks2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, tasks2, 1)
	assert.Equal(t, "poison", tasks2[0].ID)
	assert.Equal(t, []byte("x"), tasks2[0].Payload)

	require.NoError(t, consumer.Reject(ctx, "poison", WithDeadLetter()))
	purged, err := admin.PurgeDLQ(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, 1, purged)

	n, err = admin.CountDLQ(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	ti2, err := admin.InspectTask(ctx, q, "poison")
	require.NoError(t, err)
	assert.False(t, ti2.Exists)

	depthFinal, err := admin.QueueDepth(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(0), depthFinal)
}

func TestAdmin_DLQ_ConsumeRejectWithDelay(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := "dlq-consume"

	producer := NewProducer(rdb, q)
	consumer := newConsumer(rdb, q, "", false)
	consumer.SetPrefetchCount(1)

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "bad", Payload: []byte("y"), Scheduled: time.Now().Add(-time.Second),
	}))

	done := make(chan struct{})
	go func() {
		_ = consumer.Consume(ctx, func(task *Task) error {
			defer close(done)
			return NewRejectWithDelay(errors.New("fatal"), WithDeadLetter())
		})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for reject")
	}
	cancel()
	time.Sleep(50 * time.Millisecond)

	n, err := admin.CountDLQ(context.Background(), q)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	entries, err := admin.ListDLQ(context.Background(), q, 0, 0)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "bad", entries[0].ID)
}

func TestAdmin_DLQ_MaxSizeEvictsOldest(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "dlq-max"

	producer := NewProducer(rdb, q)
	consumer := newConsumer(rdb, q, "", false)
	consumer.SetPrefetchCount(1)

	for _, id := range []string{"old1", "old2", "old3"} {
		require.NoError(t, producer.Publish(ctx, &Task{
			ID: id, Payload: []byte(id), Scheduled: time.Now().Add(-time.Second),
		}))
		tasks, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		require.NoError(t, consumer.Reject(ctx, id, WithDeadLetter(2)))
		time.Sleep(2 * time.Millisecond) // развести score DLQ
	}

	n, err := admin.CountDLQ(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	entries, err := admin.ListDLQ(ctx, q, 0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	ids := map[string]bool{entries[0].ID: true, entries[1].ID: true}
	assert.False(t, ids["old1"], "oldest must be evicted")
	assert.True(t, ids["old2"])
	assert.True(t, ids["old3"])

	// meta вытесненного удалена
	ti, err := admin.InspectTask(ctx, q, "old1")
	require.NoError(t, err)
	assert.False(t, ti.Exists)

}

func TestAdmin_DLQ_MaxSizeUnlimited(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "dlq-unlim"

	producer := NewProducer(rdb, q)
	consumer := newConsumer(rdb, q, "", false)
	consumer.SetPrefetchCount(1)
	consumer.SetDLQMaxSize(1) // не должен сработать при явном 0

	for _, id := range []string{"a", "b"} {
		require.NoError(t, producer.Publish(ctx, &Task{
			ID: id, Payload: []byte(id), Scheduled: time.Now().Add(-time.Second),
		}))
		tasks, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		require.NoError(t, consumer.Reject(ctx, id, WithDeadLetter(0)))
	}

	n, err := admin.CountDLQ(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
}

func TestAdmin_DLQ_SetDLQMaxSizeDefault(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "dlq-defmax"

	producer := NewProducer(rdb, q)
	consumer := newConsumer(rdb, q, "", false)
	consumer.SetPrefetchCount(1)
	consumer.SetDLQMaxSize(1)

	for _, id := range []string{"x", "y"} {
		require.NoError(t, producer.Publish(ctx, &Task{
			ID: id, Payload: []byte(id), Scheduled: time.Now().Add(-time.Second),
		}))
		tasks, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		require.NoError(t, consumer.Reject(ctx, id, WithDeadLetter()))
		time.Sleep(2 * time.Millisecond)
	}

	n, err := admin.CountDLQ(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	entries, err := admin.ListDLQ(ctx, q, 0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "y", entries[0].ID)
}

func TestAdmin_DLQ_RetryAllAndPurgeOne(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "dlq-multi"

	producer := NewProducer(rdb, q)
	consumer := newConsumer(rdb, q, "", false)
	consumer.SetPrefetchCount(1)

	for _, id := range []string{"a", "b", "c"} {
		require.NoError(t, producer.Publish(ctx, &Task{
			ID: id, Payload: []byte(id), Scheduled: time.Now().Add(-time.Second),
		}))
		tasks, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		require.NoError(t, consumer.Reject(ctx, id, WithDeadLetter()))
	}

	n, err := admin.CountDLQ(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)

	purged, err := admin.PurgeDLQ(ctx, q, "b")
	require.NoError(t, err)
	assert.Equal(t, 1, purged)

	retried, err := admin.RetryDLQ(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, 2, retried)

	n, err = admin.CountDLQ(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		tasks, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		got[tasks[0].ID] = true
		require.NoError(t, consumer.Ack(ctx, tasks[0].ID))
	}
	assert.True(t, got["a"])
	assert.True(t, got["c"])
	assert.False(t, got["b"])
}
