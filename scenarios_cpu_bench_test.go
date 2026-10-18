package redisqueue

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// CPU / slowlog сценарии под prod-кейс «N чатов коннектора, блок → холостые Get».
// Запуск:
//
//	go test -run='^$' -bench='BenchmarkBlocked|BenchmarkParked' -benchtime=1s -count=3 .
//	go test -run='TestBlockedGroupGet_SlowlogBudget' -count=1 .
//
// Метрики (b.ReportMetric):
//   redis_cpu_us/op  — Δ(used_cpu_sys+used_cpu_user) / ops, µs процессорного времени Redis
//   eval_us/op       — COMMANDSTATS usec(eval+evalsha) / ops
//   slow_max_us      — max Duration из SLOWLOG за прогон
//   slow_n           — число записей SLOWLOG за прогон
//
// См. docs/ANALYSIS.md §1.4 / §3.8.

const (
	benchSlowlogThresholdUs = 5000 // 5ms — порог записи в SLOWLOG на время бенча
	// Жёсткий fail в Test*: при SRQ_BENCH_STRICT=1 — 5ms; иначе 20ms (шум localhost).
	slowlogBudgetLoose  = 20 * time.Millisecond
	slowlogBudgetStrict = 5 * time.Millisecond
)

type redisCPUSample struct {
	sys  float64
	user float64
}

type redisBenchMetrics struct {
	rdb             *redis.Client
	ctx             context.Context
	beforeCPU       redisCPUSample
	prevSlowThres   string
	slowlogEnabled  bool
	cmdstatsResetOK bool
}

func parseInfoCPU(info string) redisCPUSample {
	var s redisCPUSample
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if v, ok := strings.CutPrefix(line, "used_cpu_sys:"); ok {
			s.sys, _ = strconv.ParseFloat(v, 64)
		} else if v, ok := strings.CutPrefix(line, "used_cpu_user:"); ok {
			s.user, _ = strconv.ParseFloat(v, 64)
		}
	}
	return s
}

func readRedisCPU(ctx context.Context, rdb *redis.Client) redisCPUSample {
	info, err := rdb.Info(ctx, "cpu").Result()
	if err != nil {
		return redisCPUSample{}
	}
	return parseInfoCPU(info)
}

func parseEvalUsec(commandstats string) (calls int64, usec int64) {
	for _, line := range strings.Split(commandstats, "\n") {
		line = strings.TrimSpace(line)
		name, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		if name != "cmdstat_eval" && name != "cmdstat_evalsha" {
			continue
		}
		var c, u int64
		for _, part := range strings.Split(rest, ",") {
			k, v, ok := strings.Cut(part, "=")
			if !ok {
				continue
			}
			n, _ := strconv.ParseInt(v, 10, 64)
			switch k {
			case "calls":
				c = n
			case "usec":
				u = n
			}
		}
		calls += c
		usec += u
	}
	return calls, usec
}

func beginRedisBenchMetrics(ctx context.Context, rdb *redis.Client) *redisBenchMetrics {
	m := &redisBenchMetrics{rdb: rdb, ctx: ctx}
	prev, err := rdb.ConfigGet(ctx, "slowlog-log-slower-than").Result()
	if err == nil {
		if v, ok := prev["slowlog-log-slower-than"]; ok {
			m.prevSlowThres = v
		}
		if err := rdb.ConfigSet(ctx, "slowlog-log-slower-than", strconv.Itoa(benchSlowlogThresholdUs)).Err(); err == nil {
			m.slowlogEnabled = true
			_ = rdb.SlowLogReset(ctx).Err()
		}
	}
	if err := rdb.Do(ctx, "CONFIG", "RESETSTAT").Err(); err == nil {
		m.cmdstatsResetOK = true
	}
	m.beforeCPU = readRedisCPU(ctx, rdb)
	return m
}

func (m *redisBenchMetrics) report(b *testing.B) {
	b.Helper()
	if m == nil || b.N == 0 {
		return
	}
	after := readRedisCPU(m.ctx, m.rdb)
	cpuSec := (after.sys + after.user) - (m.beforeCPU.sys + m.beforeCPU.user)
	if cpuSec < 0 {
		cpuSec = 0
	}
	b.ReportMetric(cpuSec*1e6/float64(b.N), "redis_cpu_us/op")

	if m.cmdstatsResetOK {
		stats, err := m.rdb.Info(m.ctx, "commandstats").Result()
		if err == nil {
			_, usec := parseEvalUsec(stats)
			b.ReportMetric(float64(usec)/float64(b.N), "eval_us/op")
		}
	}

	if m.slowlogEnabled {
		entries, err := m.rdb.SlowLogGet(m.ctx, 128).Result()
		if err == nil {
			var maxUs float64
			for _, e := range entries {
				us := float64(e.Duration.Microseconds())
				if us > maxUs {
					maxUs = us
				}
			}
			b.ReportMetric(maxUs, "slow_max_us")
			b.ReportMetric(float64(len(entries)), "slow_n")
		}
		if m.prevSlowThres != "" {
			_ = m.rdb.ConfigSet(m.ctx, "slowlog-log-slower-than", m.prevSlowThres).Err()
		}
	}
}

func (m *redisBenchMetrics) maxSlowDuration() time.Duration {
	if m == nil || !m.slowlogEnabled {
		return 0
	}
	entries, err := m.rdb.SlowLogGet(m.ctx, 128).Result()
	if err != nil {
		return 0
	}
	var max time.Duration
	for _, e := range entries {
		if e.Duration > max {
			max = e.Duration
		}
	}
	if m.prevSlowThres != "" {
		_ = m.rdb.ConfigSet(m.ctx, "slowlog-log-slower-than", m.prevSlowThres).Err()
	}
	return max
}

func seedConnectorChats(b testing.TB, producer *Producer, parts int, group string) {
	b.Helper()
	ctx := context.Background()
	tasks := make([]*Task, parts)
	for j := 0; j < parts; j++ {
		tasks[j] = &Task{
			ID:        fmt.Sprintf("chat-%d", j),
			Partition: fmt.Sprintf("!conn:%d", j),
			Payload:   []byte("x"),
			Groups:    []string{group},
			Scheduled: time.Now().Add(-time.Second),
		}
	}
	if err := producer.Publish(ctx, tasks...); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkBlockedGroupIdleGets — группа коннектора запаркована (BlockGroups),
// в очереди ещё N ordered-чатов. Get должен сразу возвращать empty без O(N) обхода.
// Именно этот паттерн жрал Redis CPU на проде при массовой парковке.
func BenchmarkBlockedGroupIdleGets(b *testing.B) {
	partCounts := []int{100, 500, 1000}
	for _, parts := range partCounts {
		b.Run(fmt.Sprintf("parts-%d", parts), func(b *testing.B) {
			producer, consumer, rdb := setupBenchQueue(b)
			ctx := context.Background()
			const group = "conn"
			seedConnectorChats(b, producer, parts, group)
			if err := consumer.BlockGroups(ctx, 3600, group); err != nil {
				b.Fatal(err)
			}
			// Прогрев / проверка контракта
			got, err := consumer.Get(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if len(got) != 0 {
				b.Fatalf("BlockGroups(%s): want empty Get, got %d tasks", group, len(got))
			}

			m := beginRedisBenchMetrics(ctx, rdb)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := consumer.Get(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) != 0 {
					b.Fatalf("iter %d: expected empty under blocked group", i)
				}
			}
			b.StopTimer()
			m.report(b)
		})
	}
}

// BenchmarkIdleRejectConnector — prod-паттерн: Get → app rate-limit → Reject.
//
//	part_only   — Reject(delay) только на чат: до N холостых выдач задач
//	block_group — Reject(delay, group): после первого hit группа запаркована, дальше empty Get
//
// Метрика redis_cpu_us/op показывает цену «дожрать» b.N попыток Get.
func BenchmarkIdleRejectConnector(b *testing.B) {
	const parts = 500
	modes := []struct {
		name       string
		blockGroup bool
	}{
		{"part_only", false},
		{"block_group", true},
	}
	for _, mode := range modes {
		b.Run(fmt.Sprintf("%s/parts-%d", mode.name, parts), func(b *testing.B) {
			producer, consumer, rdb := setupBenchQueue(b)
			ctx := context.Background()
			const group = "conn"
			seedConnectorChats(b, producer, parts, group)

			m := beginRedisBenchMetrics(ctx, rdb)
			b.ReportAllocs()
			b.ResetTimer()
			var delivered int64
			for i := 0; i < b.N; i++ {
				got, err := consumer.Get(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) == 0 {
					continue
				}
				delivered++
				t := got[0]
				if mode.blockGroup {
					if err := consumer.Reject(ctx, t.ID, 3600, group); err != nil {
						b.Fatal(err)
					}
				} else {
					if err := consumer.Reject(ctx, t.ID, 3600); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			m.report(b)
			b.ReportMetric(float64(delivered)/float64(b.N), "deliver_frac")
		})
	}
}

// BenchmarkParkedPartitionsIdleGets — группа свободна, почти все партиции в blocked
// (как после RejectWithDelay на чат). Ищем единственную свободную / empty после парковки всех.
// Ловит деградацию линейного обхода partitions.
func BenchmarkParkedPartitionsIdleGets(b *testing.B) {
	partCounts := []int{100, 500}
	for _, parts := range partCounts {
		b.Run(fmt.Sprintf("parts-%d", parts), func(b *testing.B) {
			producer, consumer, rdb := setupBenchQueue(b)
			ctx := context.Background()
			const group = "conn"
			seedConnectorChats(b, producer, parts, group)

			// Забираем и паркуем все партиции через Reject(delay) без BlockGroups.
			parked := 0
			for parked < parts {
				got, err := consumer.Get(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) == 0 {
					b.Fatalf("empty Get while parking (%d/%d)", parked, parts)
				}
				for _, t := range got {
					if err := consumer.Reject(ctx, t.ID, 3600); err != nil {
						b.Fatal(err)
					}
					parked++
				}
			}

			m := beginRedisBenchMetrics(ctx, rdb)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := consumer.Get(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) != 0 {
					b.Fatalf("iter %d: expected empty when all partitions parked", i)
				}
			}
			b.StopTimer()
			m.report(b)
		})
	}
}

// TestConnectorRateLimitLoad_Head — S1000-style на HEAD: part_only vs block_group.
// Без BlockGroups Redis CPU остаётся высоким; с BlockGroups — резко падает, Ack≈rate.
//
//	go test -run=TestConnectorRateLimitLoad_Head -v -count=1 -timeout=5m .
func TestConnectorRateLimitLoad_Head(t *testing.T) {
	if testing.Short() {
		t.Skip("load test")
	}
	const (
		chats          = 1000
		workers        = 4
		ackRate        = 15.0
		duration       = 12 * time.Second
		rejectDelaySec = 2.0
		group          = "conn"
	)

	type modeRun struct {
		name          string
		blockGroup    bool
		redisCPUMilli float64
		ackPerSec     float64
		rejectPerSec  float64
		evalUs        float64
	}

	run := func(blockGroup bool) modeRun {
		t.Helper()
		cfg := RedisConfig{Addr: "localhost:6379", DB: 5}
		rdb, err := newClient(cfg)
		if err != nil {
			t.Skipf("Redis not available: %v", err)
		}
		ctx := context.Background()
		rdb.FlushDB(ctx)
		q := fmt.Sprintf("head-rl-%v-%d", blockGroup, time.Now().UnixNano())
		producer := NewProducer(rdb, q)
		tasks := make([]*Task, chats)
		for i := 0; i < chats; i++ {
			tasks[i] = &Task{
				ID: fmt.Sprintf("c%d", i), Partition: fmt.Sprintf("!conn:%d", i),
				Groups: []string{group}, Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
			}
		}
		for i := 0; i < len(tasks); i += 100 {
			j := i + 100
			if j > len(tasks) {
				j = len(tasks)
			}
			requireNoErr(t, producer.Publish(ctx, tasks[i:j]...))
		}

		_ = rdb.Do(ctx, "CONFIG", "RESETSTAT").Err()
		cpu0 := readRedisCPU(ctx, rdb)
		runCtx, cancel := context.WithTimeout(ctx, duration)
		defer cancel()

		lim := newScanTokenBucket(ackRate)
		var acks, rejects atomic.Int64
		var wg sync.WaitGroup
		start := time.Now()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				c := NewConsumer(rdb, q, fmt.Sprintf("w-%d", id))
				defer c.Close()
				c.SetPrefetchCount(1)
				for runCtx.Err() == nil {
					got, err := c.Get(runCtx)
					if err != nil {
						continue
					}
					if len(got) == 0 {
						// как poll у Consume/GetChan — без busy-spin пустых Get
						select {
						case <-runCtx.Done():
							return
						case <-time.After(2 * time.Millisecond):
						}
						continue
					}
					for _, task := range got {
						if lim.allow() {
							_ = c.Ack(runCtx, task.ID, 0)
							acks.Add(1)
						} else if blockGroup {
							_ = c.Reject(runCtx, task.ID, rejectDelaySec, group)
							rejects.Add(1)
						} else {
							_ = c.Reject(runCtx, task.ID, rejectDelaySec)
							rejects.Add(1)
						}
					}
				}
			}(w)
		}
		wg.Wait()
		sec := time.Since(start).Seconds()
		cpu1 := readRedisCPU(ctx, rdb)
		cpuSec := (cpu1.sys + cpu1.user) - (cpu0.sys + cpu0.user)
		if cpuSec < 0 {
			cpuSec = 0
		}
		stats, _ := rdb.Info(ctx, "commandstats").Result()
		evalCalls, evalUsec := parseEvalUsec(stats)
		avgEval := 0.0
		if evalCalls > 0 {
			avgEval = float64(evalUsec) / float64(evalCalls)
		}
		name := "part_only"
		if blockGroup {
			name = "block_group"
		}
		return modeRun{
			name: name, blockGroup: blockGroup,
			redisCPUMilli: cpuSec / sec * 1000,
			ackPerSec:     float64(acks.Load()) / sec,
			rejectPerSec:  float64(rejects.Load()) / sec,
			evalUs:        avgEval,
		}
	}

	part := run(false)
	grp := run(true)
	t.Logf("part_only:  redis_cpu=%.0fm Ack/s=%.1f Reject/s=%.0f avg_EVAL_us=%.0f",
		part.redisCPUMilli, part.ackPerSec, part.rejectPerSec, part.evalUs)
	t.Logf("block_group: redis_cpu=%.0fm Ack/s=%.1f Reject/s=%.0f avg_EVAL_us=%.0f",
		grp.redisCPUMilli, grp.ackPerSec, grp.rejectPerSec, grp.evalUs)

	// Главный сигнал отчёта: холостые Reject и дорогой EVAL, не busy-spin.
	if part.rejectPerSec < 50 {
		t.Fatalf("part_only should idle-Reject a lot, got %.0f/s", part.rejectPerSec)
	}
	if grp.rejectPerSec > 10 {
		t.Fatalf("block_group should nearly stop idle Reject, got %.0f/s (part=%.0f)",
			grp.rejectPerSec, part.rejectPerSec)
	}
	if part.evalUs < 80 {
		t.Fatalf("part_only expected expensive EVAL, avg_us=%.0f", part.evalUs)
	}
	if grp.evalUs > part.evalUs*0.35 {
		t.Fatalf("block_group EVAL should be much cheaper: part=%.0fµs group=%.0fµs",
			part.evalUs, grp.evalUs)
	}
	if grp.redisCPUMilli > part.redisCPUMilli*0.5 {
		t.Fatalf("block_group should cut Redis CPU (with poll on empty): part=%.0fm group=%.0fm",
			part.redisCPUMilli, grp.redisCPUMilli)
	}
}

func requireNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// local token bucket (same idea as partition-scan load; keep file self-contained).
type scanTokenBucket struct {
	mu     sync.Mutex
	rate   float64
	tokens float64
	last   time.Time
}

func newScanTokenBucket(rate float64) *scanTokenBucket {
	return &scanTokenBucket{rate: rate, tokens: rate, last: time.Now()}
}

func (b *scanTokenBucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	b.last = now
	if b.tokens > b.rate {
		b.tokens = b.rate
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// TestBlockedGroupGet_SlowlogBudget — алерт на «Get 77ms» класс проблем.
// По умолчанию бюджет 20ms; SRQ_BENCH_STRICT=1 → 5ms.
func TestBlockedGroupGet_SlowlogBudget(t *testing.T) {
	producer, consumer, rdb := setupCPUBenchQueue(t)
	ctx := context.Background()
	const (
		parts = 1000
		group = "conn"
		gets  = 30
	)
	seedConnectorChats(t, producer, parts, group)
	if err := consumer.BlockGroups(ctx, 3600, group); err != nil {
		t.Fatal(err)
	}

	m := beginRedisBenchMetrics(ctx, rdb)
	if !m.slowlogEnabled {
		t.Skip("cannot CONFIG SET slowlog-log-slower-than (managed Redis?)")
	}

	for i := 0; i < gets; i++ {
		got, err := consumer.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("want empty Get under BlockGroups, got %d", len(got))
		}
	}

	maxSlow := m.maxSlowDuration()
	budget := slowlogBudgetLoose
	if os.Getenv("SRQ_BENCH_STRICT") == "1" {
		budget = slowlogBudgetStrict
	}
	if maxSlow > budget {
		t.Fatalf("slowlog max Get/Lua %v > budget %v (parts=%d blocked group); redis CPU regression?",
			maxSlow, budget, parts)
	}
	t.Logf("ok: %d idle Gets, parts=%d, slowlog_max=%v (budget %v)", gets, parts, maxSlow, budget)
}

func setupCPUBenchQueue(t *testing.T) (*Producer, *Consumer, *redis.Client) {
	t.Helper()
	cfg := RedisConfig{
		Addr:     "localhost:6379",
		Password: "",
		DB:       5,
	}
	client, err := newClient(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	ctx := context.Background()
	client.FlushDB(ctx)
	producer := NewProducer(client, "bench-queue")
	consumer := NewConsumer(client, "bench-queue", "")
	t.Cleanup(func() { consumer.Close() })
	return producer, consumer, client
}
