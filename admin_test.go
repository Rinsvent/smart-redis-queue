package redisqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redis/go-redis/v9"
)

func setupAdminTest(t *testing.T) (*Admin, *redis.Client) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       5,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	ctx2 := context.Background()
	rdb.FlushDB(ctx2)
	return NewAdmin(rdb), rdb
}

func TestAdmin_ListQueues(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "admin-test-queue")
	_ = producer.Publish(ctx, &Task{
		ID:        "t1",
		Payload:   []byte("x"),
		Scheduled: time.Now(),
	})

	queues, err := admin.ListQueues(ctx)
	require.NoError(t, err)
	assert.Contains(t, queues, "admin-test-queue")
}

func TestAdmin_Inspect(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "inspect-queue")
	_ = producer.Publish(ctx,
		&Task{ID: "t1", Partition: "base", Payload: []byte("a"), Scheduled: time.Now()},
		&Task{ID: "t2", Partition: "p1", Payload: []byte("b"), Scheduled: time.Now()},
	)

	statsList, err := admin.Inspect(ctx, "inspect-queue", "")
	require.NoError(t, err)
	require.Len(t, statsList, 1)
	stats := statsList[0]
	assert.Equal(t, "inspect-queue", stats.QueueName)
	assert.Equal(t, int64(2), stats.TotalPending)
	assert.Len(t, stats.Partitions, 2)
}

func TestAdmin_Purge(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "purge-queue")
	_ = producer.Publish(ctx, &Task{ID: "t1", Payload: []byte("x"), Scheduled: time.Now()})

	n, err := admin.Purge(ctx, "purge-queue", "")
	require.NoError(t, err)
	assert.Greater(t, n, 0)

	queues, _ := admin.ListQueues(ctx)
	assert.NotContains(t, queues, "purge-queue")
}

func TestAdmin_PurgePartition(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "purge-part-queue")
	_ = producer.Publish(ctx,
		&Task{ID: "t1", Partition: "base", Payload: []byte("a"), Scheduled: time.Now()},
		&Task{ID: "t2", Partition: "p1", Payload: []byte("b"), Scheduled: time.Now()},
	)

	n, err := admin.Purge(ctx, "purge-part-queue", "p1")
	require.NoError(t, err)
	assert.Greater(t, n, 0)

	statsList, _ := admin.Inspect(ctx, "purge-part-queue", "")
	require.Len(t, statsList, 1)
	assert.Equal(t, int64(1), statsList[0].TotalPending)
}

func TestAdmin_Retry(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "retry-queue")
	consumer := newConsumer(rdb, "retry-queue", "", false)
	consumer.SetPrefetchCount(1)

	_ = producer.Publish(ctx, &Task{ID: "t1", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second)})
	tasks, _ := consumer.Get(ctx)
	require.Len(t, tasks, 1)

	n, err := admin.Retry(ctx, "retry-queue")
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	tasks2, _ := consumer.Get(ctx)
	require.Len(t, tasks2, 1)
	assert.Equal(t, "t1", tasks2[0].ID)
}

func TestAdmin_Remove_Pending(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "remove-queue")
	require.NoError(t, producer.Publish(ctx, &Task{
		ID:        "t1",
		Payload:   []byte("payload-1"),
		Scheduled: time.Now(),
		Tags:      []string{"campaign-a", "batch-1"},
	}))

	res, err := admin.Remove(ctx, "remove-queue", "t1")
	require.NoError(t, err)
	assert.Equal(t, RemovalStatusRemoved, res.Status)
	assert.Equal(t, []byte("payload-1"), res.Payload)

	// Служебные ключи и теги подчищены
	assert.Equal(t, int64(0), rdb.Exists(ctx, payloadKey("remove-queue", "t1")).Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:remove-queue:partition:t1").Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:remove-queue:priority:t1").Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:remove-queue:tags:t1").Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:remove-queue:tag:campaign-a").Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:remove-queue:tag:batch-1").Val())

	// Повторное удаление — missing
	res2, err := admin.Remove(ctx, "remove-queue", "t1")
	require.NoError(t, err)
	assert.Equal(t, RemovalStatusMissing, res2.Status)
}

func TestAdmin_Remove_InProgress(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "remove-ip-queue")
	consumer := newConsumer(rdb, "remove-ip-queue", "", false)
	consumer.SetPrefetchCount(1)

	require.NoError(t, producer.Publish(ctx, &Task{
		ID:        "t1",
		Payload:   []byte("x"),
		Scheduled: time.Now().Add(-time.Second),
		Tags:      []string{"tag-ip"},
	}))
	tasks, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	res, err := admin.Remove(ctx, "remove-ip-queue", "t1")
	require.NoError(t, err)
	assert.Equal(t, RemovalStatusInProgress, res.Status)

	// Задача и тег остаются до Ack
	assert.Equal(t, int64(1), rdb.Exists(ctx, payloadKey("remove-ip-queue", "t1")).Val())
	assert.Equal(t, int64(1), rdb.SCard(ctx, "queue:remove-ip-queue:tag:tag-ip").Val())

	require.NoError(t, consumer.Ack(ctx, "t1", 0))
	assert.Equal(t, int64(0), rdb.SCard(ctx, "queue:remove-ip-queue:tag:tag-ip").Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:remove-ip-queue:tags:t1").Val())
}

func TestAdmin_Remove_CleansEmptyPartition(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "remove-part-queue")
	require.NoError(t, producer.Publish(ctx, &Task{
		ID:        "t1",
		Partition: "p1",
		Payload:   []byte("x"),
		Scheduled: time.Now(),
	}))

	_, err := admin.Remove(ctx, "remove-part-queue", "t1")
	require.NoError(t, err)

	assert.False(t, rdb.SIsMember(ctx, "queue:remove-part-queue:partitions", "p1").Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:remove-part-queue:partition:p1:priorities").Val())
}

func TestAdmin_CountByTag(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "count-tag-queue")
	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "t1", Payload: []byte("a"), Scheduled: time.Now(), Tags: []string{"mail-1", "shared"}},
		&Task{ID: "t2", Payload: []byte("b"), Scheduled: time.Now(), Tags: []string{"mail-1"}},
		&Task{ID: "t3", Payload: []byte("c"), Scheduled: time.Now(), Tags: []string{"other"}},
	))

	n, err := admin.CountByTag(ctx, "count-tag-queue", "mail-1")
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	n, err = admin.CountByTag(ctx, "count-tag-queue", "shared")
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

func TestAdmin_RemoveByTag(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "rmtag-queue")
	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "t1", Payload: []byte("p1"), Scheduled: time.Now(), Tags: []string{"camp"}},
		&Task{ID: "t2", Payload: []byte("p2"), Scheduled: time.Now(), Tags: []string{"camp"}},
		&Task{ID: "t3", Payload: []byte("p3"), Scheduled: time.Now(), Tags: []string{"camp", "keep"}},
	))

	results, err := admin.RemoveByTag(ctx, "rmtag-queue", "camp", RemoveByTagOptions{
		Limit:         2,
		ReturnPayload: true,
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	for _, r := range results {
		assert.Equal(t, RemovalStatusRemoved, r.Status)
		assert.NotEmpty(t, r.Payload)
	}

	n, err := admin.CountByTag(ctx, "rmtag-queue", "camp")
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	// Дочищаем остаток
	results, err = admin.RemoveByTag(ctx, "rmtag-queue", "camp", RemoveByTagOptions{Limit: 10, ReturnPayload: true})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, RemovalStatusRemoved, results[0].Status)

	n, err = admin.CountByTag(ctx, "rmtag-queue", "camp")
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:rmtag-queue:tag:camp").Val())

	// Тег keep должен быть снят вместе с задачей, ключ удалён
	n, err = admin.CountByTag(ctx, "rmtag-queue", "keep")
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:rmtag-queue:tag:keep").Val())
}

func TestAdmin_RemoveByTag_PrefersRemovedOverInProgress(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "rmtag-ip-queue")
	consumer := newConsumer(rdb, "rmtag-ip-queue", "", false)
	consumer.SetPrefetchCount(1)

	// Сначала берём задачу в работу, потом добавляем pending — так inprogress стабилен.
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "inprog", Payload: []byte("ip"), Scheduled: time.Now().Add(-time.Second), Tags: []string{"mix"},
	}))
	tasks, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "inprog", tasks[0].ID)

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "pend-1", Payload: []byte("p1"), Scheduled: time.Now().Add(-time.Second), Tags: []string{"mix"}},
		&Task{ID: "pend-2", Payload: []byte("p2"), Scheduled: time.Now().Add(-time.Second), Tags: []string{"mix"}},
	))

	results, err := admin.RemoveByTag(ctx, "rmtag-ip-queue", "mix", RemoveByTagOptions{
		Limit:         2,
		ReturnPayload: true,
	})
	require.NoError(t, err)
	require.Len(t, results, 2)

	removed := 0
	for _, r := range results {
		assert.Equal(t, RemovalStatusRemoved, r.Status)
		assert.Contains(t, []string{"pend-1", "pend-2"}, r.TaskID)
		removed++
	}
	assert.Equal(t, 2, removed)

	// Осталась только in-progress в индексе тега
	n, err := admin.CountByTag(ctx, "rmtag-ip-queue", "mix")
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	results, err = admin.RemoveByTag(ctx, "rmtag-ip-queue", "mix", RemoveByTagOptions{Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, RemovalStatusInProgress, results[0].Status)
}

func TestAdmin_RemoveByTag_LimitClamp(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "rmtag-limit-queue")
	tasks := make([]*Task, 0, 5)
	for i := 0; i < 5; i++ {
		tasks = append(tasks, &Task{
			ID:        fmt.Sprintf("t-%d", i),
			Payload:   []byte("x"),
			Scheduled: time.Now(),
			Tags:      []string{"lim"},
		})
	}
	require.NoError(t, producer.Publish(ctx, tasks...))

	// Limit=0 → default 100, но задач только 5
	results, err := admin.RemoveByTag(ctx, "rmtag-limit-queue", "lim", RemoveByTagOptions{Limit: 0})
	require.NoError(t, err)
	assert.Len(t, results, 5)
}

func TestPublish_TagsForbiddenSeparator(t *testing.T) {
	_, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "bad-tag-queue")
	err := producer.Publish(ctx, &Task{
		ID:        "t1",
		Payload:   []byte("x"),
		Scheduled: time.Now(),
		Tags:      []string{"a" + TagSeparator + "b"},
	})
	require.Error(t, err)
}
