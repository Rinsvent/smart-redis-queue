package redisqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimit_SetRateLimit_Validate(t *testing.T) {
	_, consumer, _ := setupTestQueue(t)
	require.Error(t, consumer.SetRateLimit(&RateLimitConfig{
		Algorithm: "nope",
		Group:     &RateLimitLayer{Rates: []Rate{{time.Second, 1}}},
	}))
	for _, algo := range []string{
		"", RateLimitTokenBucket, RateLimitLeakyBucket, RateLimitFixedWindowCounter,
		RateLimitSlidingWindowLog, RateLimitSlidingWindowCounter,
	} {
		require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
			Algorithm: algo,
			Group:     &RateLimitLayer{Rates: []Rate{{time.Second, 10}}},
		}), algo)
	}
	require.NoError(t, consumer.SetRateLimit(nil))
}

func TestRateLimit_AllAlgorithms_GroupLimit(t *testing.T) {
	algos := []string{
		RateLimitTokenBucket, RateLimitLeakyBucket, RateLimitFixedWindowCounter,
		RateLimitSlidingWindowLog, RateLimitSlidingWindowCounter,
	}
	for _, algo := range algos {
		t.Run(algo, func(t *testing.T) {
			producer, consumer, rdb := setupTestQueue(t)
			consumer.SetPrefetchCount(1)
			require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
				Algorithm: algo,
				Group:     &RateLimitLayer{Rates: []Rate{{time.Hour, 1}}},
			}))
			ctx := context.Background()
			require.NoError(t, producer.Publish(ctx,
				&Task{ID: "a1-" + algo, Partition: "!t:c1", Groups: []string{"tok"},
					Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second)},
				&Task{ID: "a2-" + algo, Partition: "!t:c2", Groups: []string{"tok"},
					Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second)},
			))
			got, err := consumer.Get(ctx)
			require.NoError(t, err)
			require.Len(t, got, 1)
			got2, err := consumer.Get(ctx)
			require.NoError(t, err)
			assert.Empty(t, got2, algo)
			score, err := rdb.ZScore(ctx, "queue:test-queue:groups", "tok").Result()
			require.NoError(t, err)
			assert.Greater(t, score, float64(time.Now().UnixMilli()), algo)
			require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
		})
	}
}

func TestRateLimit_GroupLayer_BlocksGroup(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Group: &RateLimitLayer{Rates: []Rate{{time.Hour, 1}}},
	}))
	ctx := context.Background()
	q := "test-queue"

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "a1", Partition: "!tok:chat-1", Groups: []string{"token:1"},
			Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "a2", Partition: "!tok:chat-2", Groups: []string{"token:1"},
			Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second)},
	))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2, "group limit 1/h — вторая партиция того же token не выдаётся")

	score, err := rdb.ZScore(ctx, "queue:"+q+":groups", "token:1").Result()
	require.NoError(t, err)
	assert.Greater(t, score, float64(time.Now().UnixMilli()))

	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestRateLimit_PartitionLayer_BlocksPartition(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Partition: &RateLimitLayer{Rates: []Rate{{time.Hour, 1}}},
	}))
	ctx := context.Background()
	q := "test-queue"
	part := "user-421"

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "p1", Partition: part, Payload: []byte("1"), Scheduled: time.Now().Add(-time.Second),
	}))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "p2", Partition: part, Payload: []byte("2"), Scheduled: time.Now().Add(-time.Second),
	}))

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)

	score, err := rdb.ZScore(ctx, "queue:"+q+":blocked", part).Result()
	require.NoError(t, err)
	assert.Greater(t, score, float64(time.Now().UnixMilli()))
}

func TestRateLimit_BothLayers_GroupFirst(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Group:     &RateLimitLayer{Rates: []Rate{{time.Hour, 1}}},
		Partition: &RateLimitLayer{Rates: []Rate{{time.Hour, 100}}},
	}))
	ctx := context.Background()
	q := "test-queue"

	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "b1", Partition: "!t:c1", Groups: []string{"tok"},
			Payload: []byte("a"), Scheduled: time.Now().Add(-time.Second)},
		&Task{ID: "b2", Partition: "!t:c2", Groups: []string{"tok"},
			Payload: []byte("b"), Scheduled: time.Now().Add(-time.Second)},
	))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)

	// упёрлись в group — blocked partition для c2 может не появиться (counter partition не жгли)
	gScore, err := rdb.ZScore(ctx, "queue:"+q+":groups", "tok").Result()
	require.NoError(t, err)
	assert.Greater(t, gScore, float64(time.Now().UnixMilli()))

	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestRateLimit_Disabled_NoExtraKeys(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "n1", Partition: "p", Groups: []string{"g"},
		Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	var cursor uint64
	var rlKeys []string
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "queue:test-queue:rl:*", 100).Result()
		require.NoError(t, err)
		rlKeys = append(rlKeys, keys...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	assert.Empty(t, rlKeys)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestRateLimit_PoolInherits(t *testing.T) {
	pool := NewConsumerPool(nil, "q")
	require.NoError(t, pool.SetRateLimit(&RateLimitConfig{
		Group: &RateLimitLayer{Rates: []Rate{{time.Second, 5}}},
	}))
	require.NotNil(t, pool.rateLimit)
	require.NoError(t, pool.SetRateLimit(nil))
	assert.Nil(t, pool.rateLimit)
}

// Кейсы как в github.com/Rinsvent/rate-limiter (Allow × Value, затем deny + RetryAfter).
var rateLimitAllAlgos = []string{
	RateLimitTokenBucket, RateLimitLeakyBucket, RateLimitFixedWindowCounter,
	RateLimitSlidingWindowLog, RateLimitSlidingWindowCounter,
}

func publishReady(t *testing.T, p *Producer, tasks ...*Task) {
	t.Helper()
	past := time.Now().Add(-time.Second)
	for _, task := range tasks {
		if task.Scheduled.IsZero() {
			task.Scheduled = past
		}
		if task.Payload == nil {
			task.Payload = []byte(task.ID)
		}
	}
	require.NoError(t, p.Publish(context.Background(), tasks...))
}

func TestRateLimit_Validate_BadRates(t *testing.T) {
	_, consumer, _ := setupTestQueue(t)
	require.Error(t, consumer.SetRateLimit(&RateLimitConfig{
		Group: &RateLimitLayer{Rates: []Rate{{Duration: time.Second, Value: 0}}},
	}))
	require.Error(t, consumer.SetRateLimit(&RateLimitConfig{
		Partition: &RateLimitLayer{Rates: []Rate{{Duration: 0, Value: 1}}},
	}))
	require.Error(t, consumer.SetRateLimit(&RateLimitConfig{
		Group: &RateLimitLayer{Rates: []Rate{{Duration: -time.Second, Value: 1}}},
	}))
}

func TestRateLimit_SetRateLimit_ClonesConfig(t *testing.T) {
	_, consumer, _ := setupTestQueue(t)
	cfg := &RateLimitConfig{
		Algorithm: RateLimitLeakyBucket,
		Group:     &RateLimitLayer{Rates: []Rate{{time.Second, 3}}},
	}
	require.NoError(t, consumer.SetRateLimit(cfg))
	cfg.Algorithm = "nope"
	cfg.Group.Rates[0].Value = 99
	require.Equal(t, RateLimitLeakyBucket, consumer.rateLimit.Algorithm)
	require.Equal(t, 3, consumer.rateLimit.Group.Rates[0].Value)
}

func TestRateLimit_BurstThenDeny_AllAlgorithms(t *testing.T) {
	// Как Allow×2 + deny в оригинале; Duration=hour чтобы refill не мешал.
	for _, algo := range rateLimitAllAlgos {
		t.Run(algo, func(t *testing.T) {
			producer, consumer, rdb := setupTestQueue(t)
			consumer.SetPrefetchCount(1)
			require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
				Algorithm: algo,
				Group:     &RateLimitLayer{Rates: []Rate{{time.Hour, 2}}},
			}))
			ctx := context.Background()
			publishReady(t, producer,
				&Task{ID: "b1-" + algo, Partition: "!t:c1", Groups: []string{"g"}},
				&Task{ID: "b2-" + algo, Partition: "!t:c2", Groups: []string{"g"}},
				&Task{ID: "b3-" + algo, Partition: "!t:c3", Groups: []string{"g"}},
			)

			got1, err := consumer.Get(ctx)
			require.NoError(t, err)
			require.Len(t, got1, 1)
			got2, err := consumer.Get(ctx)
			require.NoError(t, err)
			require.Len(t, got2, 1)
			got3, err := consumer.Get(ctx)
			require.NoError(t, err)
			assert.Empty(t, got3)

			score, err := rdb.ZScore(ctx, "queue:test-queue:groups", "g").Result()
			require.NoError(t, err)
			assert.Greater(t, score, float64(time.Now().UnixMilli()))

			require.NoError(t, consumer.Ack(ctx, got1[0].ID, 0))
			require.NoError(t, consumer.Ack(ctx, got2[0].ID, 0))
		})
	}
}

func TestRateLimit_PartitionBurstThenDeny_AllAlgorithms(t *testing.T) {
	for _, algo := range rateLimitAllAlgos {
		t.Run(algo, func(t *testing.T) {
			producer, consumer, rdb := setupTestQueue(t)
			consumer.SetPrefetchCount(1)
			require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
				Algorithm: algo,
				Partition: &RateLimitLayer{Rates: []Rate{{time.Hour, 2}}},
			}))
			ctx := context.Background()
			part := "user-" + algo
			publishReady(t, producer,
				&Task{ID: "p1-" + algo, Partition: part},
				&Task{ID: "p2-" + algo, Partition: part},
				&Task{ID: "p3-" + algo, Partition: part},
			)

			var taken []string
			for i := 0; i < 2; i++ {
				got, err := consumer.Get(ctx)
				require.NoError(t, err)
				require.Len(t, got, 1)
				taken = append(taken, got[0].ID)
				require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
			}
			got3, err := consumer.Get(ctx)
			require.NoError(t, err)
			assert.Empty(t, got3)

			score, err := rdb.ZScore(ctx, "queue:test-queue:blocked", part).Result()
			require.NoError(t, err)
			assert.Greater(t, score, float64(time.Now().UnixMilli()))
			assert.Len(t, taken, 2)
		})
	}
}

func TestRateLimit_MultiRate_AND(t *testing.T) {
	// Два окна AND: широкий лимит/сек не спасает от 1/час (как rates[] в оригинале).
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Algorithm: RateLimitTokenBucket,
		Group: &RateLimitLayer{Rates: []Rate{
			{Duration: time.Second, Value: 100},
			{Duration: time.Hour, Value: 1},
		}},
	}))
	ctx := context.Background()
	publishReady(t, producer,
		&Task{ID: "m1", Partition: "!t:c1", Groups: []string{"tok"}},
		&Task{ID: "m2", Partition: "!t:c2", Groups: []string{"tok"}},
	)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)

	score, err := rdb.ZScore(ctx, "queue:test-queue:groups", "tok").Result()
	require.NoError(t, err)
	assert.Greater(t, score, float64(time.Now().UnixMilli()))
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestRateLimit_GroupIsolation_OtherGroupOk(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Group: &RateLimitLayer{Rates: []Rate{{time.Hour, 1}}},
	}))
	ctx := context.Background()
	publishReady(t, producer,
		&Task{ID: "g1a", Partition: "!a:1", Groups: []string{"g1"}},
		&Task{ID: "g1b", Partition: "!a:2", Groups: []string{"g1"}},
		&Task{ID: "g2a", Partition: "!b:1", Groups: []string{"g2"}},
	)

	var ids []string
	for i := 0; i < 5; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		if len(got) == 0 {
			continue
		}
		ids = append(ids, got[0].ID)
		require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
	}
	assert.Contains(t, ids, "g2a")
	assert.Len(t, ids, 2) // g1×1 + g2×1; второй из g1 не берём
	assert.True(t, containsAny(ids, "g1a", "g1b"))
}

func containsAny(ids []string, want ...string) bool {
	set := map[string]struct{}{}
	for _, id := range ids {
		set[id] = struct{}{}
	}
	for _, w := range want {
		if _, ok := set[w]; ok {
			return true
		}
	}
	return false
}

func TestRateLimit_PartitionIsolation_OtherPartitionOk(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Partition: &RateLimitLayer{Rates: []Rate{{time.Hour, 1}}},
	}))
	ctx := context.Background()
	publishReady(t, producer,
		&Task{ID: "pa1", Partition: "user-a"},
		&Task{ID: "pa2", Partition: "user-a"},
	)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "user-a", got[0].Partition)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	gotBlocked, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, gotBlocked)

	publishReady(t, producer, &Task{ID: "pb1", Partition: "user-b"})
	gotOther, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotOther, 1)
	assert.Equal(t, "user-b", gotOther[0].Partition)
	require.NoError(t, consumer.Ack(ctx, gotOther[0].ID, 0))
}

func TestRateLimit_BothLayers_PartitionDeny(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Group:     &RateLimitLayer{Rates: []Rate{{time.Hour, 100}}},
		Partition: &RateLimitLayer{Rates: []Rate{{time.Hour, 1}}},
	}))
	ctx := context.Background()
	part := "!t:chat"
	publishReady(t, producer,
		&Task{ID: "bp1", Partition: part, Groups: []string{"tok"}},
	)
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	publishReady(t, producer,
		&Task{ID: "bp2", Partition: part, Groups: []string{"tok"}},
	)
	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)

	score, err := rdb.ZScore(ctx, "queue:test-queue:blocked", part).Result()
	require.NoError(t, err)
	assert.Greater(t, score, float64(time.Now().UnixMilli()))
	// group не должна быть заблокирована — упёрлись в partition
	gScore, err := rdb.ZScore(ctx, "queue:test-queue:groups", "tok").Result()
	if err == nil {
		assert.LessOrEqual(t, gScore, float64(time.Now().UnixMilli()))
	}
}

func TestRateLimit_PrefetchRespectsLimit(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(10)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Group: &RateLimitLayer{Rates: []Rate{{time.Hour, 2}}},
	}))
	ctx := context.Background()
	var tasks []*Task
	for i := 0; i < 5; i++ {
		tasks = append(tasks, &Task{
			ID: fmt.Sprintf("pf-%d", i), Partition: fmt.Sprintf("!t:c%d", i),
			Groups: []string{"tok"}, Payload: []byte("x"),
		})
	}
	publishReady(t, producer, tasks...)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	for _, tm := range got {
		require.NoError(t, consumer.Ack(ctx, tm.ID, 0))
	}
}

func TestRateLimit_ResumeAfterShortWindow(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Algorithm: RateLimitTokenBucket,
		Group:     &RateLimitLayer{Rates: []Rate{{Duration: time.Second, Value: 1}}},
	}))
	ctx := context.Background()
	publishReady(t, producer,
		&Task{ID: "r1", Partition: "!t:c1", Groups: []string{"tok"}},
		&Task{ID: "r2", Partition: "!t:c2", Groups: []string{"tok"}},
	)

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	got2, err := consumer.Get(ctx)
	require.NoError(t, err)
	assert.Empty(t, got2)

	deadline := time.Now().Add(3 * time.Second)
	var got3 []*Task
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		got3, err = consumer.Get(ctx)
		require.NoError(t, err)
		if len(got3) > 0 {
			break
		}
	}
	require.Len(t, got3, 1, "после окна token-bucket должен снова отдавать")
	require.NoError(t, consumer.Ack(ctx, got3[0].ID, 0))
}

func TestRateLimit_DefaultAlgorithm_WritesTokenBucketKeys(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		// Algorithm пустой → token-bucket
		Group: &RateLimitLayer{Rates: []Rate{{time.Hour, 5}}},
	}))
	ctx := context.Background()
	publishReady(t, producer,
		&Task{ID: "dk1", Partition: "!t:c1", Groups: []string{"tok"}},
	)
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	var found bool
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "queue:test-queue:rl:g:tok:token-bucket:*", 100).Result()
		require.NoError(t, err)
		if len(keys) > 0 {
			found = true
			break
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	assert.True(t, found, "ожидали ключи rl:g:…:token-bucket:…")
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestRateLimit_NoBurnOnLockedPartition(t *testing.T) {
	// A без RL держит lock; B с лимитом 1 не должен сжечь квоту на чужом lock.
	producer, consumerA, _ := setupTestQueue(t)
	consumerB := NewConsumer(consumerA.redis, "test-queue", "consumer-b")
	t.Cleanup(func() { consumerB.Close() })
	consumerA.SetPrefetchCount(1)
	consumerB.SetPrefetchCount(1)
	require.NoError(t, consumerB.SetRateLimit(&RateLimitConfig{
		Group: &RateLimitLayer{Rates: []Rate{{time.Hour, 1}}},
	}))
	ctx := context.Background()
	publishReady(t, producer,
		&Task{ID: "lock1", Partition: "!same:1", Groups: []string{"tok"}},
	)
	gotA, err := consumerA.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotA, 1)

	publishReady(t, producer,
		&Task{ID: "other1", Partition: "!other:1", Groups: []string{"tok"}},
	)
	// !same всё ещё under A; в ready могут быть обе — B не жжёт квоту на lock.
	gotB, err := consumerB.Get(ctx)
	require.NoError(t, err)
	require.Len(t, gotB, 1, "B берёт !other; обход locked !same не жжёт квоту")
	assert.Equal(t, "other1", gotB[0].ID)
	require.NoError(t, consumerA.Ack(ctx, gotA[0].ID, 0))
	require.NoError(t, consumerB.Ack(ctx, gotB[0].ID, 0))
}

func TestRateLimit_EmptyConfigDisables(t *testing.T) {
	_, consumer, _ := setupTestQueue(t)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Group: &RateLimitLayer{Rates: []Rate{{time.Second, 1}}},
	}))
	require.NotNil(t, consumer.rateLimit)
	require.NoError(t, consumer.SetRateLimit(&RateLimitConfig{
		Group:     &RateLimitLayer{},
		Partition: &RateLimitLayer{Rates: nil},
	}))
	assert.Nil(t, consumer.rateLimit)
}
