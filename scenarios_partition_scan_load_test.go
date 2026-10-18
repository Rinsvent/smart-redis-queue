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

// Load-тест из отчёта partition-scan (staging S1000):
//   N ordered-чатов, лимит 15 Ack/с на «коннектор», остальное Reject(delay) → :block на чат.
//   Get на a96f70e: SMEMBERS + EXISTS :block на каждую партицию → O(N) Lua / Redis CPU.
//
// Совместим с a96f70e (нет Groups/BlockGroups) и с HEAD (Reject без blockGroups).
//
//	go test -run=TestPartitionScanLoad_S1000 -count=1 -timeout=5m .
//	SRQ_SCAN_CHATS=1000 SRQ_SCAN_DURATION=20s SRQ_SCAN_WORKERS=4 go test -run=TestPartitionScanLoad_S1000 -v -count=1 .
//
// Сравнение коммитов:
//	BENCH_SCAN=1 ./scripts/bench-commits.sh a96f70e HEAD

func TestPartitionScanLoad_S1000(t *testing.T) {
	chats := envInt("SRQ_SCAN_CHATS", 1000)
	workers := envInt("SRQ_SCAN_WORKERS", 4)
	ackRate := envFloat("SRQ_SCAN_ACK_RATE", 15)
	duration := envDuration("SRQ_SCAN_DURATION", 20*time.Second)
	rejectDelaySec := envInt("SRQ_SCAN_REJECT_DELAY", 2)

	cfg := RedisConfig{Addr: "localhost:6379", DB: 5}
	rdb, err := newClient(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	ctx := context.Background()
	rdb.FlushDB(ctx)

	queue := fmt.Sprintf("scan-load-%d", time.Now().UnixNano())
	producer := NewProducer(rdb, queue)

	t.Logf("seed %d chats × 1 task (queue=%s)", chats, queue)
	tasks := make([]*Task, chats)
	for i := 0; i < chats; i++ {
		tasks[i] = &Task{
			ID:        fmt.Sprintf("c%d", i),
			Partition: fmt.Sprintf("!conn:%d", i),
			Payload:   []byte("x"),
			Scheduled: time.Now().Add(-time.Second),
		}
	}
	// батчами — на a96f70e большой Publish ок, но не упираемся в arg limits
	const batch = 100
	for i := 0; i < len(tasks); i += batch {
		j := i + batch
		if j > len(tasks) {
			j = len(tasks)
		}
		if err := producer.Publish(ctx, tasks[i:j]...); err != nil {
			t.Fatal(err)
		}
	}

	prevSlow, _ := rdb.ConfigGet(ctx, "slowlog-log-slower-than").Result()
	_ = rdb.ConfigSet(ctx, "slowlog-log-slower-than", "2000").Err() // как в отчёте: 2ms
	_ = rdb.SlowLogReset(ctx).Err()
	_ = rdb.Do(ctx, "CONFIG", "RESETSTAT").Err()
	defer func() {
		if v, ok := prevSlow["slowlog-log-slower-than"]; ok {
			_ = rdb.ConfigSet(ctx, "slowlog-log-slower-than", v).Err()
		}
	}()

	cpu0 := readInfoCPU(ctx, rdb)
	runCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	lim := newTokenBucket(ackRate)
	var acks, rejects, gets, emptyGets atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := NewConsumer(rdb, queue, fmt.Sprintf("w-%d", id))
			defer c.Close()
			c.SetPrefetchCount(1)
			for {
				if runCtx.Err() != nil {
					return
				}
				got, err := c.Get(runCtx)
				if err != nil {
					if runCtx.Err() != nil {
						return
					}
					continue
				}
				gets.Add(1)
				if len(got) == 0 {
					emptyGets.Add(1)
					continue
				}
				for _, task := range got {
					if lim.allow() {
						if err := c.Ack(runCtx, task.ID, 0); err != nil && runCtx.Err() == nil {
							t.Errorf("ack: %v", err)
						}
						acks.Add(1)
					} else {
						// Только partition :block (как в отчёте до group-block).
						// waitTime — untyped const: a96f70e Reject(..., int) и HEAD Reject(..., float64).
						var err error
						switch rejectDelaySec {
						case 1:
							err = c.Reject(runCtx, task.ID, 1)
						case 5:
							err = c.Reject(runCtx, task.ID, 5)
						default:
							err = c.Reject(runCtx, task.ID, 2)
						}
						if err != nil && runCtx.Err() == nil {
							t.Errorf("reject: %v", err)
						}
						rejects.Add(1)
					}
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	cpu1 := readInfoCPU(ctx, rdb)

	stats, _ := rdb.Info(ctx, "commandstats").Result()
	evalCalls, evalUsec := cmdStat(stats, "cmdstat_evalsha")
	evalCalls2, evalUsec2 := cmdStat(stats, "cmdstat_eval")
	evalCalls += evalCalls2
	evalUsec += evalUsec2
	getCalls, _ := cmdStat(stats, "cmdstat_get")
	existsCalls, _ := cmdStat(stats, "cmdstat_exists")
	smemCalls, _ := cmdStat(stats, "cmdstat_smembers")
	incrCalls, _ := cmdStat(stats, "cmdstat_incr")
	// a96f70e: :block через EXISTS; lock/payload — GET. В отчёте staging «Lua-GET» ≈ probes+reads.
	probeCalls := getCalls + existsCalls

	cpuSec := (cpu1.sys + cpu1.user) - (cpu0.sys + cpu0.user)
	if cpuSec < 0 {
		cpuSec = 0
	}
	sec := elapsed.Seconds()
	redisCPUMilli := cpuSec / sec * 1000
	evalPerSec := float64(evalCalls) / sec
	avgEvalUs := 0.0
	if evalCalls > 0 {
		avgEvalUs = float64(evalUsec) / float64(evalCalls)
	}

	slowEntries, _ := rdb.SlowLogGet(ctx, 128).Result()
	var slowMax time.Duration
	slowN := 0
	for _, e := range slowEntries {
		slowN++
		if e.Duration > slowMax {
			slowMax = e.Duration
		}
	}

	t.Logf("--- partition-scan load (report S1000-style) ---")
	t.Logf("chats=%d workers=%d ack_rate=%.0f/s duration=%v reject_delay=%ds",
		chats, workers, ackRate, duration, rejectDelaySec)
	t.Logf("wall=%.1fs  redis_cpu=%.0fm  (staging S1000≈450m)", sec, redisCPUMilli)
	t.Logf("EVALSHA/s=%.0f  avg_EVAL_us=%.0f  (staging≈1690/s, 240us)", evalPerSec, avgEvalUs)
	t.Logf("probe/s(GET+EXISTS)=%.0f  GET/s=%.0f EXISTS/s=%.0f  SMEMBERS/s=%.0f  INCR/s=%.0f",
		float64(probeCalls)/sec, float64(getCalls)/sec, float64(existsCalls)/sec,
		float64(smemCalls)/sec, float64(incrCalls)/sec)
	t.Logf("  (staging Lua-GET≈154k, SMEMBERS≈570 — на a96f70e :block=EXISTS)")
	t.Logf("app: Get/s=%.0f empty_Get/s=%.0f Ack/s=%.1f Reject/s=%.0f",
		float64(gets.Load())/sec, float64(emptyGets.Load())/sec,
		float64(acks.Load())/sec, float64(rejects.Load())/sec)
	t.Logf("slowlog(>2ms): n=%d max=%v  (staging max Get 77ms)", slowN, slowMax)

	// «Поймали сканирование» на a96f70e: дорогой EVAL + высокий Redis CPU + много probe.
	// На HEAD с groups/blocked — не ставить SRQ_EXPECT_SCAN_COST (цифры ниже).
	if os.Getenv("SRQ_EXPECT_SCAN_COST") == "1" {
		if avgEvalUs < 80 {
			t.Fatalf("expected expensive EVAL on partition-scan path, avg_EVAL_us=%.0f (<80)", avgEvalUs)
		}
		if redisCPUMilli < 100 {
			t.Fatalf("expected Redis CPU pressure, got %.0fm (<100m)", redisCPUMilli)
		}
		if float64(probeCalls)/sec < 2000 {
			t.Fatalf("expected high :block/:lock probes (GET+EXISTS)/s, got %.0f/s", float64(probeCalls)/sec)
		}
	}
}

type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	tokens float64
	last   time.Time
}

func newTokenBucket(rate float64) *tokenBucket {
	return &tokenBucket{rate: rate, tokens: rate, last: time.Now()}
}

func (b *tokenBucket) allow() bool {
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

type infoCPU struct{ sys, user float64 }

func readInfoCPU(ctx context.Context, rdb *redis.Client) infoCPU {
	info, err := rdb.Info(ctx, "cpu").Result()
	if err != nil {
		return infoCPU{}
	}
	var s infoCPU
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "used_cpu_sys:") {
			s.sys, _ = strconv.ParseFloat(strings.TrimPrefix(line, "used_cpu_sys:"), 64)
		} else if strings.HasPrefix(line, "used_cpu_user:") {
			s.user, _ = strconv.ParseFloat(strings.TrimPrefix(line, "used_cpu_user:"), 64)
		}
	}
	return s
}

func cmdStat(commandstats, name string) (calls, usec int64) {
	for _, line := range strings.Split(commandstats, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, name+":") {
			continue
		}
		rest := strings.TrimPrefix(line, name+":")
		for _, part := range strings.Split(rest, ",") {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) != 2 {
				continue
			}
			n, _ := strconv.ParseInt(kv[1], 10, 64)
			switch kv[0] {
			case "calls":
				calls = n
			case "usec":
				usec = n
			}
		}
	}
	return calls, usec
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envFloat(k string, def float64) float64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func envDuration(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
