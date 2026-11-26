package redisqueue

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Кейс с прода: партиции с отложенной головой (Reject delay / Scheduled) остаются в
// group:g:ready и каждый idle Get их перебирает → Redis CPU.
//
// До фикса ready score=0 → ZCOUNT -inf now == N, budget/slowlog растут с N.
// После: score=readyAt → ZCOUNT -inf now == 0, idle Get дешёвый.
//
//	go test -run='TestReadyScore_|TestFutureHead' -count=1 -v .
//	go test -run='^$' -bench='BenchmarkFutureHeadInReady' -benchtime=2s -count=3 .

func seedFutureHeadChats(t testing.TB, producer *Producer, parts int, group string, when time.Time) {
	t.Helper()
	ctx := context.Background()
	tasks := make([]*Task, parts)
	for j := 0; j < parts; j++ {
		tasks[j] = &Task{
			ID:        fmt.Sprintf("fut-chat-%d", j),
			Partition: fmt.Sprintf("!conn:%d", j),
			Payload:   []byte("x"),
			Groups:    []string{group},
			Scheduled: when,
		}
	}
	require.NoError(t, producer.Publish(ctx, tasks...))
}

func parkAllOrderedWithRejectDelay(t testing.TB, consumer *Consumer, parts int, delaySec float64) {
	t.Helper()
	ctx := context.Background()
	parked := 0
	for parked < parts {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, got, "empty while parking %d/%d", parked, parts)
		for _, task := range got {
			require.NoError(t, consumer.Reject(ctx, task.ID, delaySec))
			parked++
		}
	}
}

// TestReadyScore_RejectDelay_ReadyAtInFuture — после Reject(delay) на ordered
// партиция в ready со score=unlockAt; due-срез пуст; idle Get не жжёт budget.
func TestReadyScore_RejectDelay_ReadyAtInFuture(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(10)
	ctx := context.Background()
	const (
		parts = 200
		group = "conn"
		q     = "test-queue"
	)
	seedConnectorChats(t, producer, parts, group)
	parkAllOrderedWithRejectDelay(t, consumer, parts, 3600)

	readyKey := "queue:" + q + ":group:" + group + ":ready"
	nowMs := float64(time.Now().UnixMilli())
	if tm, err := rdb.Time(ctx).Result(); err == nil {
		nowMs = float64(tm.UnixMilli())
	}

	card, err := rdb.ZCard(ctx, readyKey).Result()
	require.NoError(t, err)
	require.Greater(t, card, int64(0), "паркованные остаются в ready")

	due, err := rdb.ZCount(ctx, readyKey, "-inf", strconv.FormatInt(int64(nowMs), 10)).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), due, "due-срез должен быть пуст (score=unlockAt); card=%d", card)

	members, err := rdb.ZRange(ctx, readyKey, 0, 0).Result()
	require.NoError(t, err)
	require.NotEmpty(t, members)
	score, err := rdb.ZScore(ctx, readyKey, members[0]).Result()
	require.NoError(t, err)
	assert.Greater(t, score, nowMs, "ready score должен быть unlockAt > now")

	consumer.SetPartitionScanBudget(8)
	for i := 0; i < 5; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		assert.Empty(t, got)
	}
	assert.Equal(t, int64(0), consumer.LastPartitionBudgetHits(),
		"idle Get не должен сканировать parked через budget")
}

// TestReadyScore_PublishScheduled_ReadyAtIsScheduled — Publish(Scheduled=future)
// кладёт партицию в ready со score=scheduled, не 0.
func TestReadyScore_PublishScheduled_ReadyAtIsScheduled(t *testing.T) {
	producer, _, rdb := setupTestQueue(t)
	ctx := context.Background()
	future := time.Now().Add(30 * time.Minute)
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "later", Partition: "!p1", Groups: []string{"g"},
		Payload: []byte("x"), Scheduled: future,
	}))

	score, err := rdb.ZScore(ctx, "queue:test-queue:group:g:ready", "!p1").Result()
	require.NoError(t, err)
	assert.InDelta(t, float64(future.UnixMilli()), score, 2000)

	nowMs := float64(time.Now().UnixMilli())
	due, err := rdb.ZCount(ctx, "queue:test-queue:group:g:ready", "-inf", strconv.FormatInt(int64(nowMs), 10)).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), due)
}

// TestReadyScore_FutureHeads_IdleGetBudget — много future в ready + idle Gets
// не должны попадать в slowlog (после фикса score).
func TestReadyScore_FutureHeads_IdleGetBudget(t *testing.T) {
	producer, consumer, rdb := setupCPUBenchQueue(t)
	ctx := context.Background()
	const (
		parts = 1000
		group = "conn"
		gets  = 40
	)
	seedFutureHeadChats(t, producer, parts, group, time.Now().Add(time.Hour))

	readyKey := "queue:bench-queue:group:" + group + ":ready"
	nowMs := float64(time.Now().UnixMilli())
	if tm, err := rdb.Time(ctx).Result(); err == nil {
		nowMs = float64(tm.UnixMilli())
	}
	due, err := rdb.ZCount(ctx, readyKey, "-inf", strconv.FormatInt(int64(nowMs), 10)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), due, "future не должны быть due")

	m := beginRedisBenchMetrics(ctx, rdb)
	if !m.slowlogEnabled {
		t.Skip("cannot CONFIG SET slowlog-log-slower-than")
	}
	consumer.SetPartitionScanBudget(32)
	for i := 0; i < gets; i++ {
		got, err := consumer.Get(ctx)
		require.NoError(t, err)
		require.Empty(t, got)
	}
	assert.Equal(t, int64(0), consumer.LastPartitionBudgetHits())

	maxSlow := m.maxSlowDuration()
	budget := slowlogBudgetLoose
	if maxSlow > budget {
		t.Fatalf("slowlog max %v > budget %v (parts=%d future heads in ready)", maxSlow, budget, parts)
	}
	t.Logf("ok: %d idle Gets, parts=%d future, slowlog_max=%v", gets, parts, maxSlow)
}

// TestReadyScore_OneDueAmongFutures — среди тысячи future находится одна due.
func TestReadyScore_OneDueAmongFutures(t *testing.T) {
	producer, consumer, _ := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	const group = "conn"
	seedFutureHeadChats(t, producer, 300, group, time.Now().Add(time.Hour))
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "due-1", Partition: "!due", Groups: []string{group},
		Payload: []byte("now"), Scheduled: time.Now().Add(-time.Second),
	}))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "due-1", got[0].ID)
}

// BenchmarkFutureHeadInReadyIdleGets — idle Get при N future-head в ready.
// До score-fix: eval_us растёт с N; после — почти flat.
func BenchmarkFutureHeadInReadyIdleGets(b *testing.B) {
	for _, parts := range []int{200, 1000, 3000} {
		b.Run(fmt.Sprintf("parts-%d", parts), func(b *testing.B) {
			producer, consumer, rdb := setupBenchQueue(b)
			ctx := context.Background()
			const group = "conn"
			seedFutureHeadChats(b, producer, parts, group, time.Now().Add(time.Hour))

			m := beginRedisBenchMetrics(ctx, rdb)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := consumer.Get(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) != 0 {
					b.Fatalf("expected empty, got %d", len(got))
				}
			}
			b.StopTimer()
			m.report(b)
		})
	}
}

// BenchmarkRejectDelayParkedIdleGets — idle Get после Reject(delay) на N ordered.
// Тот же прод-кейс Messenger (~5k parked).
func BenchmarkRejectDelayParkedIdleGets(b *testing.B) {
	for _, parts := range []int{200, 1000} {
		b.Run(fmt.Sprintf("parts-%d", parts), func(b *testing.B) {
			producer, consumer, rdb := setupBenchQueue(b)
			ctx := context.Background()
			const group = "conn"
			seedConnectorChats(b, producer, parts, group)
			parkAllOrderedWithRejectDelay(b, consumer, parts, 3600)

			m := beginRedisBenchMetrics(ctx, rdb)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := consumer.Get(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) != 0 {
					b.Fatalf("expected empty, got %d", len(got))
				}
			}
			b.StopTimer()
			m.report(b)
		})
	}
}
