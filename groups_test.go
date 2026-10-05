package redisqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueue_Groups_BlockGroupsSkipsWholeGroup(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "a1", Partition: "!connA:1", Groups: []string{"connA"}, Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "b1", Partition: "!connB:1", Groups: []string{"connB"}, Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second)},
	))

	require.NoError(t, consumer.BlockGroups(ctx, 2, "connA"))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "b1", got[0].ID, "должна взяться только свободная группа")
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2, "connA ещё заблокирована")

	_, err = rdb.ZScore(ctx, queueBlockedKey("test-queue"), "g:connA").Result()
	require.NoError(t, err)
}

func TestQueue_Groups_RejectWithBlockGroups(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "a1", Partition: "!connA:1", Groups: []string{"connA"}, Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "a2", Partition: "!connA:2", Groups: []string{"connA"}, Payload: []byte("a2"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "b1", Partition: "!connB:1", Groups: []string{"connB"}, Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second)},
	))

	var fromA *Task
	for i := 0; i < 5; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, got)
		if got[0].Partition == "!connA:1" || got[0].Partition == "!connA:2" {
			fromA = got[0]
			break
		}
		require.NoError(t, consumer.Reject(ctx, got[0].ID, 0))
	}
	require.NotNil(t, fromA, "нужна задача из connA")

	require.NoError(t, consumer.Reject(ctx, fromA.ID, 1.0, "connA"))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "b1", got[0].ID, "после BlockGroups(connA) доступен только connB")
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)
}

func TestQueue_Groups_MultiGroupAdmission(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	// Партиция в двух группах; блокируем одну — admission должен отсечь
	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "shared", Partition: "!p-shared", Groups: []string{"g1", "g2"}, Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "only-g1", Partition: "!p-g1", Groups: []string{"g1"}, Payload: []byte("y"), Scheduled: time.Now().Add(-time.Second)},
	))

	require.NoError(t, consumer.BlockGroups(ctx, 2, "g2"))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "only-g1", got[0].ID, "shared в g2 не должен выдаваться")
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)
}

func TestQueue_Groups_LegacyBlockKeyStillHonored(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "t1", Partition: "!legacy", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	require.NoError(t, rdb.Set(ctx, partitionBlockKey("test-queue", "!legacy"), "1", 2*time.Second).Err())

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got)

	require.NoError(t, rdb.Del(ctx, partitionBlockKey("test-queue", "!legacy")).Err())
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got2)
	require.NoError(t, consumer.Ack(ctx, got2[0].ID, 0))
}

func TestAdmin_BlockGroups(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()

	producer := NewProducer(rdb, "admin-groups")
	consumer := newConsumer(rdb, "admin-groups", "", false)
	consumer.SetPrefetchCount(1)

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "t1", Partition: "!c:1", Groups: []string{"c"}, Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	require.NoError(t, admin.BlockGroups(ctx, "admin-groups", 5, "c"))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestQueue_Groups_ScaleBlockedGroup: сотни тысяч партиций в заблокированной группе
// не должны мешать быстрому Get из маленькой свободной группы.
func TestQueue_Groups_ScaleBlockedGroup(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test")
	}
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	const blockedN = 100_000
	q := "test-queue"
	blockedGroup := "conn-blocked"
	freeGroup := "conn-free"

	pipe := rdb.Pipeline()
	for i := 0; i < blockedN; i++ {
		part := fmt.Sprintf("!blocked:%d", i)
		pipe.SAdd(ctx, "queue:"+q+":groups", blockedGroup)
		pipe.SAdd(ctx, "queue:"+q+":group:"+blockedGroup+":ready", part)
		pipe.SAdd(ctx, "queue:"+q+":partition:"+part+":groups", blockedGroup)
		pipe.SAdd(ctx, "queue:"+q+":partitions", part)
		if i%5000 == 4999 {
			_, err := pipe.Exec(ctx)
			require.NoError(t, err)
			pipe = rdb.Pipeline()
		}
	}
	_, err := pipe.Exec(ctx)
	require.NoError(t, err)

	unlockAt := float64(time.Now().Add(time.Hour).UnixMilli())
	require.NoError(t, rdb.ZAdd(ctx, queueBlockedKey(q), redis.Z{
		Score: unlockAt, Member: "g:" + blockedGroup,
	}).Err())

	require.NoError(t, producer.Publish(ctx, &Task{
		ID:        "needle",
		Partition: "!free:needle",
		Groups:    []string{freeGroup},
		Payload:   []byte("found"),
		Scheduled: time.Now().Add(-time.Second),
	}))

	start := time.Now()
	got, err := consumer.Get(ctx)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "needle", got[0].ID)
	t.Logf("Get with %d partitions in blocked group took %s", blockedN, elapsed)
	assert.Less(t, elapsed, 2*time.Second, "Get не должен сканировать заблокированную группу")
	require.NoError(t, consumer.Ack(ctx, "needle", 0))
}

func TestQueue_Groups_GetSpeedVsLegacyScan(t *testing.T) {
	if testing.Short() {
		t.Skip("speed test")
	}
	// Фиксируем, что group-walk остаётся быстрым при большой заблокированной группе.
	// (Полный legacy-скан O(N) GET здесь не гоняем — он заведомо хуже на тех же данных.)
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	q := "test-queue"

	const n = 50_000
	pipe := rdb.Pipeline()
	for i := 0; i < n; i++ {
		part := fmt.Sprintf("!parked:%d", i)
		pipe.SAdd(ctx, "queue:"+q+":groups", "parked")
		pipe.SAdd(ctx, "queue:"+q+":group:parked:ready", part)
		pipe.SAdd(ctx, "queue:"+q+":partition:"+part+":groups", "parked")
		if i%5000 == 0 {
			_, err := pipe.Exec(ctx)
			require.NoError(t, err)
			pipe = rdb.Pipeline()
		}
	}
	_, err := pipe.Exec(ctx)
	require.NoError(t, err)
	require.NoError(t, rdb.ZAdd(ctx, queueBlockedKey(q), redis.Z{
		Score: float64(time.Now().Add(time.Hour).UnixMilli()), Member: "g:parked",
	}).Err())

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "live", Partition: "!live:1", Groups: []string{"live"},
		Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))

	const rounds = 20
	var total time.Duration
	for i := 0; i < rounds; i++ {
		// возвращаем задачу в очередь между замерами
		if i > 0 {
			require.NoError(t, producer.Publish(ctx, &Task{
				ID: fmt.Sprintf("live-%d", i), Partition: "!live:1", Groups: []string{"live"},
				Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
			}))
		}
		start := time.Now()
		got, err := consumer.Get(ctx)
		total += time.Since(start)
		require.NoError(t, err)
		require.NotEmpty(t, got)
		require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
	}
	avg := total / rounds
	t.Logf("avg Get over %d rounds with %d parked partitions: %s", rounds, n, avg)
	assert.Less(t, avg, 200*time.Millisecond)
}
