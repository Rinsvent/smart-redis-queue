package redisqueue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSteps_SkipAfterCommitAndReject(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPollInterval(20 * time.Millisecond)
	ctx := context.Background()
	queue := "test-queue"

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "step-1", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))

	var order1, api1 atomic.Int32
	tasks, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	s, err := NewSteps(ctx, tasks[0])
	require.NoError(t, err)
	require.Equal(t, "", s.Last())

	require.NoError(t, s.Do(ctx, "create_order", func(ctx context.Context) error {
		order1.Add(1)
		return nil
	}))
	require.Equal(t, "create_order", s.Last())
	assert.Equal(t, "create_order", rdb.Get(ctx, stepKey(queue, "step-1")).Val())

	err = s.Do(ctx, "call_api", func(ctx context.Context) error {
		api1.Add(1)
		return errors.New("api down")
	})
	require.Error(t, err)
	require.Equal(t, "create_order", s.Last())

	require.NoError(t, consumer.Reject(ctx, tasks[0].ID, 0))

	tasks2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, tasks2, 1)

	s2, err := NewSteps(ctx, tasks2[0])
	require.NoError(t, err)
	require.Equal(t, "create_order", s2.Last())

	var order2, api2, notify2 atomic.Int32
	require.NoError(t, s2.Do(ctx, "create_order", func(ctx context.Context) error {
		order2.Add(1)
		return nil
	}))
	require.NoError(t, s2.Do(ctx, "call_api", func(ctx context.Context) error {
		api2.Add(1)
		return nil
	}))
	require.NoError(t, s2.Do(ctx, "notify", func(ctx context.Context) error {
		notify2.Add(1)
		return nil
	}))

	assert.Equal(t, int32(1), order1.Load())
	assert.Equal(t, int32(1), api1.Load())
	assert.Equal(t, int32(0), order2.Load())
	assert.Equal(t, int32(1), api2.Load())
	assert.Equal(t, int32(1), notify2.Load())

	require.NoError(t, consumer.Ack(ctx, tasks2[0].ID, 0))
	assert.Equal(t, int64(0), rdb.Exists(ctx, stepKey(queue, "step-1")).Val())
}

func TestSteps_ManualCommitAndLast(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	ctx := context.Background()
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "man-1", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	tasks, err := consumer.Get(ctx)
	require.NoError(t, err)

	s, err := NewSteps(ctx, tasks[0])
	require.NoError(t, err)
	require.NoError(t, s.Commit(ctx, "prepared"))
	assert.Equal(t, "prepared", s.Last())

	var ran atomic.Int32
	require.NoError(t, s.Do(ctx, "prepared", func(ctx context.Context) error {
		ran.Add(1)
		return nil
	}))
	assert.Equal(t, int32(0), ran.Load())
	require.NoError(t, s.Do(ctx, "next", func(ctx context.Context) error {
		ran.Add(1)
		return nil
	}))
	assert.Equal(t, int32(1), ran.Load())
	require.NoError(t, consumer.Ack(ctx, tasks[0].ID, 0))
}

func TestSteps_ReleaseAndTTL(t *testing.T) {
	_, _, rdb := setupTestQueue(t)
	ctx := context.Background()

	s, err := NewStepsRaw(ctx, rdb, "test-queue", "ttl-1", StepsOptions{TTL: time.Hour})
	require.NoError(t, err)
	require.NoError(t, s.Commit(ctx, "a"))
	ttl := rdb.TTL(ctx, stepKey("test-queue", "ttl-1")).Val()
	assert.Greater(t, ttl, time.Minute)

	require.NoError(t, s.Release(ctx))
	assert.Equal(t, int64(0), rdb.Exists(ctx, stepKey("test-queue", "ttl-1")).Val())
	assert.Equal(t, "", s.Last())
}

func TestSteps_RequiresGetBind(t *testing.T) {
	ctx := context.Background()
	_, err := NewSteps(ctx, &Task{ID: "x"})
	require.Error(t, err)
}

func TestSteps_ConsumeExample(t *testing.T) {
	producer, pool, _ := setupTestConsumerPool(t)
	pool.SetCount(1)
	pool.SetPollInterval(20 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "pool-step", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))

	var skippedOK atomic.Bool
	done := make(chan struct{})
	go func() {
		pool.Consume(ctx, func(task *Task) error {
			s, err := NewSteps(ctx, task)
			if err != nil {
				return err
			}
			if s.Last() == "" {
				_ = s.Do(ctx, "a", func(ctx context.Context) error { return nil })
				return errors.New("fail once")
			}
			var aRan atomic.Int32
			_ = s.Do(ctx, "a", func(ctx context.Context) error {
				aRan.Add(1)
				return nil
			})
			_ = s.Do(ctx, "b", func(ctx context.Context) error { return nil })
			if aRan.Load() == 0 {
				skippedOK.Store(true)
			}
			cancel()
			return nil
		})
		close(done)
	}()
	<-done
	assert.True(t, skippedOK.Load())
}
