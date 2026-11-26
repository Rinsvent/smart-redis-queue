package redisqueue

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReadyScore_Scale1M_FutureAndParkedIdleCPU — прод-кейс в большом масштабе:
// ~1e6 партиций с головой в будущем (scheduled / reject-delay) в group:ready,
// плюс частые idle Get. До ready-score Get был O(N); после — ZCOUNT due=0 ≈ O(1).
//
//	go test -run=TestReadyScore_Scale1M_FutureAndParkedIdleCPU -count=1 -timeout=45m -v .
//	SRQ_READY_SCALE_N=1000000 SRQ_READY_SCALE_GETS=200 SRQ_READY_SCALE_WORKERS=8 \
//	  go test -run=TestReadyScore_Scale1M_FutureAndParkedIdleCPU -count=1 -timeout=45m -v .
func TestReadyScore_Scale1M_FutureAndParkedIdleCPU(t *testing.T) {
	if testing.Short() {
		t.Skip("scale 1M")
	}
	parts := envInt("SRQ_READY_SCALE_N", 1_000_000)
	gets := envInt("SRQ_READY_SCALE_GETS", 200)
	workers := envInt("SRQ_READY_SCALE_WORKERS", 8)
	if workers < 1 {
		workers = 1
	}

	producer, consumer, rdb := setupCPUBenchQueue(t)
	ctx := context.Background()
	q := "bench-queue"
	const group = "conn"
	futureMs := float64(time.Now().Add(2 * time.Hour).UnixMilli())

	t.Logf("seeding %d future-head partitions into %s:group:%s:ready …", parts, q, group)
	seedStart := time.Now()
	require.NoError(t, rdb.ZAdd(ctx, "queue:"+q+":groups", redis.Z{Score: 0, Member: group}).Err())

	pipe := rdb.Pipeline()
	const batch = 10_000
	for i := 0; i < parts; i++ {
		part := fmt.Sprintf("!conn:%d", i)
		// Как после Publish(Scheduled=future) / Reject(delay): member в ready со score=readyAt.
		// Половина ещё и в blocked — имитация ordered Reject(delay).
		pipe.ZAdd(ctx, "queue:"+q+":group:"+group+":ready", redis.Z{Score: futureMs, Member: part})
		if i%2 == 0 {
			pipe.ZAdd(ctx, "queue:"+q+":blocked", redis.Z{Score: futureMs, Member: part})
		}
		if i%batch == batch-1 {
			_, err := pipe.Exec(ctx)
			require.NoError(t, err)
			pipe = rdb.Pipeline()
			if (i+1)%100_000 == 0 {
				t.Logf("  seeded %d / %d (%s)", i+1, parts, time.Since(seedStart).Round(time.Millisecond))
			}
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		require.NoError(t, err)
	}
	card, err := rdb.ZCard(ctx, "queue:"+q+":group:"+group+":ready").Result()
	require.NoError(t, err)
	require.Equal(t, int64(parts), card)
	t.Logf("seed done in %s (ready card=%d)", time.Since(seedStart).Round(time.Millisecond), card)

	nowMs := float64(time.Now().UnixMilli())
	if tm, err := rdb.Time(ctx).Result(); err == nil {
		nowMs = float64(tm.UnixMilli())
	}
	due, err := rdb.ZCount(ctx, "queue:"+q+":group:"+group+":ready", "-inf", strconv.FormatInt(int64(nowMs), 10)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), due, "due-срез должен быть пуст при score=future")

	// Baseline: пустая вторая группа — для сравнения порядка величины.
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "warmup", Partition: "!warm", Groups: []string{"other"},
		Payload: []byte("w"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	m := beginRedisBenchMetrics(ctx, rdb)
	var (
		wallNs   atomic.Int64
		emptyOK  atomic.Int64
		nonEmpty atomic.Int64
	)
	var wg sync.WaitGroup
	perWorker := gets
	if perWorker < workers {
		perWorker = 1
	}
	totalGets := perWorker * workers
	t.Logf("running %d idle Gets (%d workers × %d) against %d future/parked …", totalGets, workers, perWorker, parts)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := NewConsumer(rdb, q, fmt.Sprintf("scale-w-%d", id))
			c.SetPrefetchCount(1)
			c.SetPartitionScanBudget(64)
			defer c.Close()
			for i := 0; i < perWorker; i++ {
				start := time.Now()
				got, err := c.Get(ctx)
				wallNs.Add(time.Since(start).Nanoseconds())
				if err != nil {
					t.Errorf("worker %d Get: %v", id, err)
					return
				}
				if len(got) == 0 {
					emptyOK.Add(1)
				} else {
					nonEmpty.Add(1)
					for _, task := range got {
						_ = c.Ack(ctx, task.ID, 0)
					}
				}
			}
		}(w)
	}
	wg.Wait()

	after := readRedisCPU(ctx, rdb)
	cpuSec := (after.sys + after.user) - (m.beforeCPU.sys + m.beforeCPU.user)
	if cpuSec < 0 {
		cpuSec = 0
	}
	var evalUsec int64
	if m.cmdstatsResetOK {
		stats, err := rdb.Info(ctx, "commandstats").Result()
		require.NoError(t, err)
		_, evalUsec = parseEvalUsec(stats)
	}
	avgWall := time.Duration(wallNs.Load() / int64(totalGets))
	avgEvalUs := float64(evalUsec) / float64(totalGets)
	cpuUsPerGet := cpuSec * 1e6 / float64(totalGets)

	maxSlow := m.maxSlowDuration()
	t.Logf("results parts=%d gets=%d empty=%d nonempty=%d", parts, totalGets, emptyOK.Load(), nonEmpty.Load())
	t.Logf("  avg_wall=%s  avg_eval_us=%.1f  redis_cpu_us/get=%.1f  slowlog_max=%v",
		avgWall, avgEvalUs, cpuUsPerGet, maxSlow)

	assert.Equal(t, int64(totalGets), emptyOK.Load(), "все Get должны быть пустыми (нет due)")
	// До фикса на 1k уже ~4ms eval; на 1M было бы секунды. Бюджет с запасом для localhost.
	assert.Less(t, avgWall, 10*time.Millisecond, "avg Get wall must stay O(1), not O(N)")
	assert.Less(t, avgEvalUs, 2000.0, "avg EVAL usec must stay flat (~tens–hundreds µs), got %.0f", avgEvalUs)
	if maxSlow > 0 {
		budget := 50 * time.Millisecond
		if os.Getenv("SRQ_BENCH_STRICT") == "1" {
			budget = 10 * time.Millisecond
		}
		assert.LessOrEqual(t, maxSlow, budget, "slowlog max Get")
	}

	// Needle: одна due среди миллиона future — должна находиться быстро.
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "needle", Partition: "!needle", Groups: []string{group},
		Payload: []byte("found"), Scheduled: time.Now().Add(-time.Second),
	}))
	needleStart := time.Now()
	got, err = consumer.Get(ctx)
	needleElapsed := time.Since(needleStart)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "needle", got[0].ID)
	t.Logf("needle Get among %d future took %s", parts, needleElapsed)
	assert.Less(t, needleElapsed, 2*time.Second)
	require.NoError(t, consumer.Ack(ctx, "needle", 0))

	// Индекс не разрушен idle Get'ами.
	card2, err := rdb.ZCard(ctx, "queue:"+q+":group:"+group+":ready").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(parts), card2, "future members must remain after idle Gets")
}
