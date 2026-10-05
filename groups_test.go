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

	score, err := rdb.ZScore(ctx, "queue:test-queue:groups", "connA").Result()
	require.NoError(t, err)
	assert.Greater(t, score, float64(time.Now().UnixMilli()))
}

func TestQueue_Groups_RejectWithBlockGroups(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "a1", Partition: "!connA:1", Groups: []string{"connA"}, Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "a2", Partition: "!connA:2", Groups: []string{"connA"}, Payload: []byte("a2"), Scheduled: time.Now().Add(-time.Second)},
	))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Contains(t, []string{"!connA:1", "!connA:2"}, got[0].Partition)

	require.NoError(t, consumer.Reject(ctx, got[0].ID, 1.0, "connA"))

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "b1", Partition: "!connB:1", Groups: []string{"connB"}, Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second),
	}))

	gotB, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotB, 1)
	assert.Equal(t, "b1", gotB[0].ID, "после BlockGroups(connA) доступен только connB")
	require.NoError(t, consumer.Ack(ctx, gotB[0].ID, 0))

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2, "connA ещё заблокирована")
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

func TestQueue_Get_CleansEmptyPartitionIndexes(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	q := "test-queue"

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "only", Partition: "!c:1", Groups: []string{"conn"},
		Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	// Индексы чистятся лениво на следующем Get (found=false → removePartitionFromIndexes).
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)

	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:"+q+":partition:!c:1:groups").Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:"+q+":group:conn:ready").Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:"+q+":partition:!c:1:priorities").Val())
	assert.Equal(t, int64(0), rdb.Exists(ctx, "queue:"+q+":partition:!c:1:0").Val())
	n, err := rdb.ZCard(ctx, "queue:"+q+":groups").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	assert.False(t, rdb.SIsMember(ctx, "queue:"+q+":partitions", "!c:1").Val())

	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestQueue_Ping_RestoresDeadConsumerTaskWithGroups(t *testing.T) {
	producer, setupConsumer, rdb := setupTestQueue(t)
	ctx := context.Background()
	q := "test-queue"
	// setupConsumer успел взять unlock:lock — снимаем, иначе ping c2 выйдет сразу
	require.NoError(t, rdb.Del(ctx, "queue:"+q+":unlock:lock").Err())
	require.NoError(t, rdb.SRem(ctx, "queue:"+q+":consumers", setupConsumer.ConsumerID()).Err())
	_ = setupConsumer

	c1 := newConsumer(rdb, q, "dead-c1", false)
	c1.SetPrefetchCount(1)
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "t1", Partition: "!c:1", Groups: []string{"conn"},
		Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := c1.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	// симулируем смерть: убираем heartbeat, оставляем задачу in-progress
	require.NoError(t, rdb.Del(ctx, "queue:"+q+":consumer:dead-c1").Err())

	c2 := newConsumer(rdb, q, "alive-c2", false)
	c2.SetPrefetchCount(1)
	require.NoError(t, c2.ping(ctx)) // должен вернуть задачу в очередь + ready

	got2, err := c2.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	assert.Equal(t, "t1", got2[0].ID)
	require.NoError(t, c2.Ack(ctx, "t1", 0))
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

	unlockAt := float64(time.Now().Add(time.Hour).UnixMilli())
	pipe := rdb.Pipeline()
	pipe.ZAdd(ctx, "queue:"+q+":groups", redis.Z{Score: unlockAt, Member: blockedGroup})
	for i := 0; i < blockedN; i++ {
		part := fmt.Sprintf("!blocked:%d", i)
		pipe.ZAdd(ctx, "queue:"+q+":group:"+blockedGroup+":ready", redis.Z{Score: 0, Member: part})
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
	unlockAt := float64(time.Now().Add(time.Hour).UnixMilli())
	pipe := rdb.Pipeline()
	pipe.ZAdd(ctx, "queue:"+q+":groups", redis.Z{Score: unlockAt, Member: "parked"})
	for i := 0; i < n; i++ {
		part := fmt.Sprintf("!parked:%d", i)
		pipe.ZAdd(ctx, "queue:"+q+":group:parked:ready", redis.Z{Score: 0, Member: part})
		pipe.SAdd(ctx, "queue:"+q+":partition:"+part+":groups", "parked")
		if i%5000 == 4999 {
			_, err := pipe.Exec(ctx)
			require.NoError(t, err)
			pipe = rdb.Pipeline()
		}
	}
	_, err := pipe.Exec(ctx)
	require.NoError(t, err)

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
