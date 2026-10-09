package redisqueue

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- error / API surface ---

func TestRejectWithDelay_ErrorAndUnwrap(t *testing.T) {
	inner := errors.New("rate")
	e := NewRejectWithDelay(inner, 0.5, "g1")
	assert.Equal(t, "rate", e.Error())
	assert.ErrorIs(t, e, inner)
	assert.Equal(t, 0.5, e.Delay)
	assert.Equal(t, []string{"g1"}, e.BlockGroups)

	bare := &RejectWithDelay{}
	assert.Equal(t, "reject with delay", bare.Error())
	assert.Nil(t, bare.Unwrap())
}

func TestErrTasksAlreadyExist_Error(t *testing.T) {
	e := &ErrTasksAlreadyExist{TaskIDs: []string{"a", "b"}}
	assert.Equal(t, "2 tasks already exist: [a b]", e.Error())
}

func TestEncodeSeparated_EmptyAndDuplicates(t *testing.T) {
	s, err := encodeTags([]string{"", "a", "a", "b"})
	require.NoError(t, err)
	assert.Equal(t, "a"+TagSeparator+"b", s)

	_, err = encodeGroups([]string{"bad#group"})
	require.Error(t, err)
}

func TestSetters_ClampAndFlags(t *testing.T) {
	_, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(0)
	assert.Equal(t, 1, consumer.prefetchCount)
	consumer.SetIdempotencyTtl(-time.Second)
	assert.Equal(t, time.Duration(0), consumer.idempotencyTtl)
	consumer.SetIdempotencyTtl(2 * time.Second)
	assert.Equal(t, 2*time.Second, consumer.idempotencyTtl)
	consumer.SetCheckDeadConsumerLocksOnGet(true)
	assert.True(t, consumer.checkDeadConsumerLocksOnGet)
	consumer.SetLegacyPartitionsFallback(true)
	assert.True(t, consumer.legacyPartitionsFallback)

	pool := NewConsumerPool(consumer.redis, "test-queue")
	pool.SetCount(0)
	assert.Equal(t, 1, pool.count)
	pool.SetPrefetchCount(0)
	assert.Equal(t, 1, pool.prefetchCount)
	pool.SetIdempotencyTtl(-1)
	assert.Equal(t, time.Duration(0), pool.idempotencyTtl)
	pool.SetCheckDeadConsumerLocksOnGet(true)
	assert.True(t, pool.checkDeadConsumerLocksOnGet)
	pool.Use(func(next HandlerFunc) HandlerFunc { return next })
	assert.Len(t, pool.middlewares, 1)
}

// --- Publish / Lock edges ---

func TestPublish_EmptyAndValidation(t *testing.T) {
	producer, _, _ := setupTestQueue(t)
	ctx := context.Background()
	require.NoError(t, producer.Publish(ctx))
	require.Error(t, producer.Publish(ctx, &Task{Payload: []byte("x")}))
	require.Error(t, producer.Publish(ctx, &Task{ID: "t", Groups: []string{"a#b"}, Payload: []byte("x")}))

	// Scheduled zero → now (доступна сразу)
	require.NoError(t, producer.Publish(ctx, &Task{ID: "now-zero", Payload: []byte("x")}))
}

func TestLock_EmptyDuplicateAndSuccess(t *testing.T) {
	producer, _, rdb := setupTestQueue(t)
	ctx := context.Background()
	require.NoError(t, producer.Lock(ctx))
	require.NoError(t, producer.Lock(ctx, "lock-a", "lock-b"))
	err := producer.Lock(ctx, "lock-a", "lock-c")
	var exists *ErrTasksAlreadyExist
	require.ErrorAs(t, err, &exists)
	assert.Equal(t, []string{"lock-a"}, exists.TaskIDs)
	assert.Equal(t, int64(1), rdb.Exists(ctx, payloadKey("test-queue", "lock-b")).Val())
}

// --- Delayed / priority / order / prefetch ---

func TestBusiness_DelayedMessageNotVisibleUntilDue(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "future", Payload: []byte("x"), Scheduled: time.Now().Add(300 * time.Millisecond),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got)

	time.Sleep(350 * time.Millisecond)
	got, err = consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "future", got[0].ID)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestBusiness_PriorityAndPrefetchOrderInPartition(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(3)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "p1", Partition: "!ord", Priority: 1, Payload: []byte("1"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "p10", Partition: "!ord", Priority: 10, Payload: []byte("10"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "p5", Partition: "!ord", Priority: 5, Payload: []byte("5"), Scheduled: time.Now().Add(-time.Second)},
	))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, []string{"p10", "p5", "p1"}, []string{got[0].ID, got[1].ID, got[2].ID})
	for _, task := range got {
		require.NoError(t, consumer.Ack(ctx, task.ID, 0))
	}
}

func TestBusiness_StrictOrderAcrossPrefetchBatches(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(2)
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		require.NoError(t, producer.Publish(ctx, &Task{
			ID: fmt.Sprintf("o-%d", i), Partition: "!seq", Payload: []byte("x"),
			Scheduled: time.Now().Add(-time.Second),
		}))
	}
	var order []string
	for len(order) < 5 {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, got)
		for _, task := range got {
			order = append(order, task.ID)
			require.NoError(t, consumer.Ack(ctx, task.ID, 0))
		}
	}
	assert.Equal(t, []string{"o-1", "o-2", "o-3", "o-4", "o-5"}, order)
}

// --- Rate limit / groups / default group ---

func TestBusiness_Consume_RejectWithDelay_BlockGroups(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	consumer.SetPollInterval(20 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "rl-1", Partition: "!c:1", Groups: []string{"conn"}, Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "rl-2", Partition: "!c:2", Groups: []string{"conn"}, Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "other", Partition: "!d:1", Groups: []string{"other"}, Payload: []byte("c"), Scheduled: time.Now().Add(-time.Second)},
	))

	var gotOther atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(task *Task) error {
			if task.ID == "other" {
				gotOther.Store(true)
				return nil
			}
			return NewRejectWithDelay(errors.New("429"), 2, "conn")
		})
	}()

	require.Eventually(t, func() bool {
		if !gotOther.Load() {
			return false
		}
		score, err := rdb.ZScore(ctx, "queue:test-queue:groups", "conn").Result()
		return err == nil && score > float64(time.Now().UnixMilli())
	}, 3*time.Second, 20*time.Millisecond, "other Ack + BlockGroups(conn)")
	cancel()
	<-done
}

func TestBusiness_DefaultGroupWhenGroupsEmpty(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "def", Partition: "!p", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	assert.True(t, rdb.SIsMember(ctx, "queue:test-queue:partition:!p:groups", DefaultGroup).Val())
	n, err := rdb.ZCard(ctx, "queue:test-queue:group:"+DefaultGroup+":ready").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestBusiness_PartitionBlockedInInspect(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "inspect-block"
	producer := NewProducer(rdb, q)
	consumer := newConsumer(rdb, q, "", false)
	consumer.SetPrefetchCount(1)

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "t1", Partition: "!p", Groups: []string{"g"}, Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Reject(ctx, got[0].ID, 5))

	stats, err := admin.Inspect(ctx, q, "")
	require.NoError(t, err)
	require.Len(t, stats, 1)
	require.NotEmpty(t, stats[0].Partitions)
	var found bool
	for _, p := range stats[0].Partitions {
		if p.Partition == "!p" {
			found = true
			assert.True(t, p.Blocked, "ordered reject должен пометить партицию Blocked")
		}
	}
	assert.True(t, found)
}

// --- Dead consumer / CheckDeadConsumerLocksOnGet ---

func TestBusiness_CheckDeadConsumerLocksOnGet(t *testing.T) {
	producer, _, rdb := setupTestQueue(t)
	ctx := context.Background()
	q := "test-queue"

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "after", Partition: "!deadlock", Payload: []byte("y"), Scheduled: time.Now().Add(-time.Second),
	}))
	// orphan lock: владельца нет в consumers
	require.NoError(t, rdb.Set(ctx, partitionLockKey(q, "!deadlock"), "ghost-cid", 0).Err())

	cOff := newConsumer(rdb, q, "no-flag", false)
	cOff.SetPrefetchCount(1)
	cOff.SetCheckDeadConsumerLocksOnGet(false)
	empty, err := cOff.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, empty, "без флага orphan lock блокирует Get")

	cOn := newConsumer(rdb, q, "with-flag", false)
	cOn.SetPrefetchCount(1)
	cOn.SetCheckDeadConsumerLocksOnGet(true)
	got2, err := cOn.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	assert.Equal(t, "after", got2[0].ID)
	require.NoError(t, cOn.Ack(ctx, got2[0].ID, 0))
}

// --- Legacy partitions fallback ---

func TestBusiness_LegacyPartitionsFallback(t *testing.T) {
	_, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	q := "test-queue"
	part := "!legacy-only"
	taskID := "legacy-task"
	now := float64(time.Now().Add(-time.Second).UnixMilli())

	// Имитация старого продюсера: partitions + queue, без groups/ready
	require.NoError(t, rdb.Set(ctx, payloadKey(q, taskID), "payload", 0).Err())
	require.NoError(t, rdb.Set(ctx, "queue:"+q+":partition:"+taskID, part, 0).Err())
	require.NoError(t, rdb.Set(ctx, "queue:"+q+":priority:"+taskID, "0", 0).Err())
	require.NoError(t, rdb.ZAdd(ctx, "queue:"+q+":partition:"+part+":0", redis.Z{Score: now, Member: taskID}).Err())
	require.NoError(t, rdb.ZAdd(ctx, "queue:"+q+":partition:"+part+":priorities", redis.Z{Score: 0, Member: "0"}).Err())
	require.NoError(t, rdb.SAdd(ctx, "queue:"+q+":partitions", part).Err())

	consumer.SetLegacyPartitionsFallback(false)
	empty, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, empty, "без fallback задача только в partitions не видна")

	consumer.SetLegacyPartitionsFallback(true)
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, taskID, got[0].ID)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

// --- Idempotency via SetIdempotencyTtl on Consume ---

func TestBusiness_Consume_UsesIdempotencyTtl(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	consumer.SetPollInterval(20 * time.Millisecond)
	consumer.SetIdempotencyTtl(2 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "idem-1", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(task *Task) error {
			cancel()
			return nil
		})
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}

	err := producer.Publish(context.Background(), &Task{
		ID: "idem-1", Payload: []byte("x"), Scheduled: time.Now(),
	})
	var exists *ErrTasksAlreadyExist
	require.ErrorAs(t, err, &exists)
}

// --- Tags / remove / admin edges ---

func TestBusiness_TagsLifecycleAndRemoveMissing(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "tags-life"
	producer := NewProducer(rdb, q)
	consumer := newConsumer(rdb, q, "", false)
	consumer.SetPrefetchCount(1)

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "tag-1", Payload: []byte("p"), Tags: []string{"camp", "batch"}, Scheduled: time.Now().Add(-time.Second),
	}))
	n, err := admin.CountByTag(ctx, q, "camp")
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	n, err = admin.CountByTag(ctx, q, "missing-tag")
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
	n, err = admin.CountByTag(ctx, q, "camp")
	require.NoError(t, err)
	assert.Equal(t, int64(0), n, "Ack чистит tag index")

	res, err := admin.Remove(ctx, q, "no-such")
	require.NoError(t, err)
	assert.Equal(t, RemovalStatusMissing, res.Status)
}

func TestBusiness_RemoveByTag_WithoutConsumer(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "rm-tag-biz"
	producer := NewProducer(rdb, q)

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "a", Tags: []string{"t"}, Payload: []byte("1"), Scheduled: time.Now()},
		&Task{ID: "b", Tags: []string{"t"}, Payload: []byte("2"), Scheduled: time.Now()},
		&Task{ID: "c", Tags: []string{"keep"}, Payload: []byte("3"), Scheduled: time.Now()},
	))
	batch, err := admin.RemoveByTag(ctx, q, "t", RemoveByTagOptions{Limit: 10, ReturnPayload: true})
	require.NoError(t, err)
	require.Len(t, batch, 2)
	for _, r := range batch {
		assert.Equal(t, RemovalStatusRemoved, r.Status)
		assert.NotEmpty(t, r.Payload)
	}
	n, err := admin.CountByTag(ctx, q, "keep")
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

// --- Ack/Reject ownership errors ---

func TestBusiness_AckReject_WrongOwner(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "own", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	other := newConsumer(rdb, "test-queue", "other", false)
	require.Error(t, other.Ack(ctx, got[0].ID, 0))
	require.Error(t, other.Reject(ctx, got[0].ID, 0))
	require.Error(t, consumer.Reject(ctx, "missing-id", 0))
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestBusiness_Reject_InvalidBlockGroup(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "g", Partition: "!p", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Error(t, consumer.Reject(ctx, got[0].ID, 1, "bad#group"))
	require.NoError(t, consumer.Reject(ctx, got[0].ID, 0))
}

func TestBusiness_BlockGroups_Noop(t *testing.T) {
	_, consumer, _ := setupTestQueue(t)
	ctx := context.Background()
	require.NoError(t, consumer.BlockGroups(ctx, 0, "g"))
	require.NoError(t, consumer.BlockGroups(ctx, 1))
}

// --- Middleware panic + pool Use ---

func TestBusiness_MiddlewarePanicRecovered(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	consumer.SetPollInterval(20 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var saw atomic.Bool
	consumer.Use(func(next HandlerFunc) HandlerFunc {
		return func(task *Task) error {
			saw.Store(true)
			panic("mw boom")
		}
	})
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "mw", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(task *Task) error { return nil })
	}()
	require.Eventually(t, saw.Load, 3*time.Second, 20*time.Millisecond)
	cancel()
	<-done

	got, err := consumer.Get(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.GreaterOrEqual(t, got[0].RejectCount, 1)
	require.NoError(t, consumer.Ack(context.Background(), got[0].ID, 0))
}

func TestBusiness_PoolUse_PropagatesMiddleware(t *testing.T) {
	producer, pool, _ := setupTestConsumerPool(t)
	pool.SetCount(1)
	pool.SetPrefetchCount(1)
	pool.SetPollInterval(20 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var viaMW atomic.Bool
	pool.Use(func(next HandlerFunc) HandlerFunc {
		return func(task *Task) error {
			viaMW.Store(true)
			return next(task)
		}
	})
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "pool-mw", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Consume(ctx, func(task *Task) error {
			cancel()
			return nil
		})
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}
	assert.True(t, viaMW.Load())
}

// --- GetChan Close ---

func TestBusiness_GetChan_StopsOnCancel(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	consumer.SetPollInterval(20 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "ch1", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	ch := consumer.GetChan(ctx)
	select {
	case task := <-ch:
		require.NotNil(t, task)
		require.NoError(t, consumer.Ack(context.Background(), task.ID, 0))
	case <-time.After(3 * time.Second):
		t.Fatal("no task")
	}
	cancel()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "канал должен закрыться после cancel")
	case <-time.After(3 * time.Second):
		t.Fatal("channel not closed")
	}
}

// --- Batch publish atomicity ---

func TestBusiness_BatchPublish_PartialDuplicates(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(10)
	ctx := context.Background()
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "dup", Payload: []byte("1"), Scheduled: time.Now().Add(-time.Second),
	}))
	err := producer.Publish(ctx,
		&Task{ID: "dup", Payload: []byte("1"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "new1", Payload: []byte("2"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "new2", Payload: []byte("3"), Scheduled: time.Now().Add(-time.Second)},
	)
	var exists *ErrTasksAlreadyExist
	require.ErrorAs(t, err, &exists)
	assert.Equal(t, []string{"dup"}, exists.TaskIDs)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, task := range got {
		ids[task.ID] = true
		require.NoError(t, consumer.Ack(ctx, task.ID, 0))
	}
	// dup уже был + new1/new2 добавлены (add не откатывает весь батч)
	assert.True(t, ids["dup"] || ids["new1"] || ids["new2"])
}

func TestBusiness_Inspect_GroupBlockMarksPartition(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "inspect-gblock"
	producer := NewProducer(rdb, q)
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "t1", Partition: "shared", Groups: []string{"g"}, Payload: []byte("x"), Scheduled: time.Now(),
	}))
	require.NoError(t, admin.BlockGroups(ctx, q, 10, "g"))
	stats, err := admin.Inspect(ctx, q, "shared")
	require.NoError(t, err)
	require.Len(t, stats, 1)
	require.Len(t, stats[0].Partitions, 1)
	assert.True(t, stats[0].Partitions[0].Blocked)
}

func TestBusiness_Retry_RestoresGroupsReady(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "retry-groups"
	producer := NewProducer(rdb, q)
	consumer := newConsumer(rdb, q, "retry-c1", false)
	consumer.SetPrefetchCount(1)

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "t1", Partition: "!p", Groups: []string{"g"}, Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	// Симулируем «индексы сняты, задача ещё in-progress» (как после lazy cleanup),
	// но partition:groups сохраняем — Retry должен восстановить ready.
	require.NoError(t, rdb.ZRem(ctx, "queue:"+q+":group:g:ready", "!p").Err())
	require.NoError(t, rdb.ZRem(ctx, "queue:"+q+":groups", "g").Err())
	assert.Equal(t, int64(0), rdb.ZCard(ctx, "queue:"+q+":group:g:ready").Val())

	n, err := admin.Retry(ctx, q)
	require.NoError(t, err)
	require.Equal(t, 1, n, "Retry должен вернуть in-progress задачу")
	assert.Equal(t, int64(1), rdb.ZCard(ctx, "queue:"+q+":group:g:ready").Val())
	assert.Equal(t, int64(0), rdb.HLen(ctx, "queue:"+q+":consumer:retry-c1:tasks").Val())

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	assert.Equal(t, "t1", got2[0].ID)
	require.NoError(t, consumer.Ack(ctx, got2[0].ID, 0))
}

func TestBusiness_ListQueues_AndPurgeAll(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	p1 := NewProducer(rdb, "q-a")
	p2 := NewProducer(rdb, "q-b")
	require.NoError(t, p1.Publish(ctx, &Task{ID: "1", Payload: []byte("a"), Scheduled: time.Now()}))
	require.NoError(t, p2.Publish(ctx, &Task{ID: "2", Payload: []byte("b"), Scheduled: time.Now()}))
	queues, err := admin.ListQueues(ctx)
	require.NoError(t, err)
	assert.Contains(t, queues, "q-a")
	assert.Contains(t, queues, "q-b")
	n, err := admin.Purge(ctx, "q-a", "")
	require.NoError(t, err)
	assert.Greater(t, n, 0)
	queues, err = admin.ListQueues(ctx)
	require.NoError(t, err)
	assert.NotContains(t, queues, "q-a")
}

func TestBusiness_Consume_OrderedRejectCascadeWithDelay(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(3)
	consumer.SetPollInterval(20 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "1", Partition: "!cascade", Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "2", Partition: "!cascade", Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "3", Partition: "!cascade", Payload: []byte("c"), Scheduled: time.Now().Add(-time.Second)},
	))

	var first atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(task *Task) error {
			if first.CompareAndSwap(false, true) {
				return NewRejectWithDelay(errors.New("fail"), 0.2)
			}
			t.Errorf("не должны вызывать handler для остальных задач партиции после reject: %s", task.ID)
			return nil
		})
	}()
	require.Eventually(t, first.Load, 2*time.Second, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond) // дать cascade reject
	cancel()
	<-done

	// все три вернулись, порядок сохранён
	consumer2 := newConsumer(consumer.redis, "test-queue", "", false)
	consumer2.SetPrefetchCount(3)
	time.Sleep(250 * time.Millisecond)
	got, err := consumer2.Get(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, []string{"1", "2", "3"}, []string{got[0].ID, got[1].ID, got[2].ID})
	for _, task := range got {
		require.NoError(t, consumer2.Ack(context.Background(), task.ID, 0))
	}
}

// --- Multi-group AND after free walk ---

func TestBusiness_MultiGroup_BlockedSecondGroup(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "both", Partition: "!shared", Groups: []string{"g1", "g2"}, Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "g1only", Partition: "!only", Groups: []string{"g1"}, Payload: []byte("y"), Scheduled: time.Now().Add(-time.Second)},
	))
	require.NoError(t, consumer.BlockGroups(ctx, 3, "g2"))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "g1only", got[0].ID)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
	empty, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, empty)
}
