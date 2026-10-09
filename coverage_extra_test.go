package redisqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCountByTag_Validation(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	_, err := admin.CountByTag(ctx, "q", "")
	require.Error(t, err)
	_, err = admin.CountByTag(ctx, "q", "bad#tag")
	require.Error(t, err)
}

func TestRemoveByTag_Validation(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	_, err := admin.RemoveByTag(ctx, "q", "", RemoveByTagOptions{})
	require.Error(t, err)
	_, err = admin.RemoveByTag(ctx, "q", "a#b", RemoveByTagOptions{})
	require.Error(t, err)
}

func TestPurge_EmptyQueue(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	n, err := admin.Purge(context.Background(), "no-such-queue", "")
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestPurgePartition_WithTags(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "purge-tagged"
	p := NewProducer(rdb, q)
	require.NoError(t, p.Publish(ctx,
		&Task{ID: "t1", Partition: "px", Tags: []string{"tag-a"}, Payload: []byte("1"), Scheduled: time.Now()},
		&Task{ID: "t2", Partition: "py", Tags: []string{"tag-b"}, Payload: []byte("2"), Scheduled: time.Now()},
	))
	n, err := admin.Purge(ctx, q, "px")
	require.NoError(t, err)
	assert.Greater(t, n, 0)
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:"+q+":tag:tag-a").Val())
	assert.Equal(t, int64(1), rdb.SCard(ctx, "queue:"+q+":tag:tag-b").Val())
}

func TestInspect_PartitionFilterAndInProgress(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "insp-filter"
	p := NewProducer(rdb, q)
	c := newConsumer(rdb, q, "insp-c", false)
	c.SetPrefetchCount(1)
	require.NoError(t, p.Publish(ctx,
		&Task{ID: "a", Partition: "keep", Payload: []byte("1"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "b", Partition: "skip", Payload: []byte("2"), Scheduled: time.Now().Add(-time.Second)},
	))
	got, err := c.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got)

	stats, err := admin.Inspect(ctx, q, got[0].Partition)
	require.NoError(t, err)
	require.Len(t, stats, 1)
	require.Len(t, stats[0].Partitions, 1)
	assert.Equal(t, got[0].Partition, stats[0].Partitions[0].Partition)
	assert.GreaterOrEqual(t, stats[0].Partitions[0].InProgress, int64(1))
	assert.Contains(t, stats[0].Consumers, "insp-c")
}

func TestConsume_RejectWithDelay_ExtractsFields(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	consumer.SetPollInterval(20 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "delay-g", Partition: "!p", Groups: []string{"g"}, Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(task *Task) error {
			return NewRejectWithDelay(errors.New("slow"), 1.5, "g")
		})
	}()

	require.Eventually(t, func() bool {
		score, err := rdb.ZScore(context.Background(), "queue:test-queue:groups", "g").Result()
		return err == nil && score > float64(time.Now().UnixMilli())
	}, 3*time.Second, 20*time.Millisecond)
	cancel()
	<-done

	score, err := rdb.ZScore(context.Background(), queueBlockedKey("test-queue"), "!p").Result()
	require.NoError(t, err)
	assert.Greater(t, score, float64(time.Now().UnixMilli()))
}

func TestAck_IdempotencyViaSetter(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	consumer.SetIdempotencyTtl(2 * time.Second) // EXPIRE принимает целые секунды
	ctx := context.Background()
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "ttl-set", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, consumer.idempotencyTtl))
	err = producer.Publish(ctx, &Task{ID: "ttl-set", Payload: []byte("x"), Scheduled: time.Now()})
	var exists *ErrTasksAlreadyExist
	require.ErrorAs(t, err, &exists)
}

func TestGetChan_PollsUntilCancel(t *testing.T) {
	_, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	consumer.SetPollInterval(30 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	ch := consumer.GetChan(ctx)
	time.Sleep(80 * time.Millisecond) // пустые poll-итерации
	cancel()
	select {
	case _, ok := <-ch:
		assert.False(t, ok)
	case <-time.After(2 * time.Second):
		t.Fatal("GetChan did not close")
	}
}

func TestPool_SettersPropagate(t *testing.T) {
	producer, pool, _ := setupTestConsumerPool(t)
	pool.SetCount(1)
	pool.SetPrefetchCount(2)
	pool.SetPollInterval(20 * time.Millisecond)
	pool.SetIdempotencyTtl(time.Second)
	pool.SetCheckDeadConsumerLocksOnGet(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "pool-set", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
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
}

func TestPublish_ZeroScheduledUsesNow(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	require.NoError(t, producer.Publish(ctx, &Task{ID: "zs", Payload: []byte("x")}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestBlockGroups_InvalidGroup(t *testing.T) {
	_, consumer, _ := setupTestQueue(t)
	require.Error(t, consumer.BlockGroups(context.Background(), 1, "bad#g"))
}

func TestRetry_EmptyConsumers(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	n, err := admin.Retry(context.Background(), "empty-q")
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestRemove_MissingAndScriptStatuses(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "rm-extra"
	p := NewProducer(rdb, q)
	c := newConsumer(rdb, q, "", false)
	c.SetPrefetchCount(1)
	require.NoError(t, p.Publish(ctx, &Task{
		ID: "alive", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := c.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	res, err := admin.Remove(ctx, q, "alive")
	require.NoError(t, err)
	assert.Equal(t, RemovalStatusInProgress, res.Status)
	require.NoError(t, c.Ack(ctx, got[0].ID, 0))
}
