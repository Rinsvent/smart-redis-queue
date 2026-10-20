package redisqueue

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Шардинг-бенчи. Запуск:
//
//	go test -run='^$' -bench='BenchmarkShard' -benchtime=2s -count=3 -timeout=20m .
//
// Multi-process (×N Lua): поднять N redis и передать адреса:
//
//	SRQ_SHARD_ADDRS=127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381 \
//	  go test -run='^$' -bench='BenchmarkShardPublish_MultiProcess' -benchtime=2s -count=3 .

func flushShardClients(b *testing.B, clients []*redis.Client) {
	b.Helper()
	ctx := context.Background()
	for _, c := range clients {
		if err := c.FlushDB(ctx).Err(); err != nil {
			b.Fatalf("FlushDB: %v", err)
		}
	}
}

func clientsSameHostDBs(b *testing.B, dbs ...int) []*redis.Client {
	b.Helper()
	out := make([]*redis.Client, len(dbs))
	for i, db := range dbs {
		c, err := newClient(RedisConfig{Addr: "localhost:6379", DB: db})
		if err != nil {
			b.Skipf("Redis not available: %v", err)
		}
		out[i] = c
		b.Cleanup(func() { _ = c.Close() })
	}
	flushShardClients(b, out)
	return out
}

func clientsFromAddrs(b *testing.B, addrs []string) []*redis.Client {
	b.Helper()
	out := make([]*redis.Client, len(addrs))
	for i, addr := range addrs {
		c, err := newClient(RedisConfig{Addr: addr, DB: 0})
		if err != nil {
			b.Skipf("Redis %s not available: %v", addr, err)
		}
		out[i] = c
		b.Cleanup(func() { _ = c.Close() })
	}
	flushShardClients(b, out)
	return out
}

func parseShardAddrsEnv() []string {
	raw := strings.TrimSpace(os.Getenv("SRQ_SHARD_ADDRS"))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func shardCfgClients(strategy ShardStrategy, clients []*redis.Client) *ShardConfig {
	shards := make([]Shard, len(clients))
	for i, c := range clients {
		shards[i] = Shard{Name: fmt.Sprintf("s%d", i), Client: c}
	}
	return &ShardConfig{Strategy: strategy, Shards: shards}
}

func newShardedBenchProducer(b *testing.B, cfg *ShardConfig, queue string) *Producer {
	b.Helper()
	p := NewProducer(nil, queue)
	if err := p.SetShards(cfg); err != nil {
		b.Fatalf("SetShards: %v", err)
	}
	b.Cleanup(func() { _ = p.Close() })
	return p
}

// parallelPublishByGroup — параллельные Publish с разными Groups (sticky ByGroup).
func parallelPublishByGroup(b *testing.B, producer *Producer, workers int) {
	b.Helper()
	ctx := context.Background()
	var seq atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		w := int(seq.Add(1))
		group := fmt.Sprintf("acct:%d", w%workers)
		i := 0
		for pb.Next() {
			id := fmt.Sprintf("t-%d-%d", w, i)
			i++
			_ = producer.Publish(ctx, &Task{
				ID:        id,
				Groups:    []string{group},
				Payload:   []byte("bench"),
				Scheduled: time.Now(),
			})
		}
	})
}

// BenchmarkShardPublish_SingleDB — baseline: один Redis DB, без SetShards.
func BenchmarkShardPublish_SingleDB(b *testing.B) {
	clients := clientsSameHostDBs(b, 12)
	p := NewProducer(clients[0], "bench-shard-single")
	parallelPublishByGroup(b, p, 8)
}

// BenchmarkShardPublish_MultiDB — N logical DB на одном процессе (Lua всё ещё serial).
// Имеет смысл для меньшего числа ключей на DB; ×N Lua не ждём.
func BenchmarkShardPublish_MultiDB(b *testing.B) {
	for _, n := range []int{2, 4} {
		b.Run(fmt.Sprintf("db_%d", n), func(b *testing.B) {
			dbs := make([]int, n)
			for i := 0; i < n; i++ {
				dbs[i] = 12 + i
			}
			clients := clientsSameHostDBs(b, dbs...)
			cfg := shardCfgClients(ShardByGroup, clients)
			p := newShardedBenchProducer(b, cfg, "bench-shard-multidb")
			parallelPublishByGroup(b, p, 8)
		})
	}
}

// BenchmarkShardPublish_MultiProcess — N отдельных redis-server (×N Lua).
// Требует SRQ_SHARD_ADDRS=host:port,...
func BenchmarkShardPublish_MultiProcess(b *testing.B) {
	addrs := parseShardAddrsEnv()
	if len(addrs) < 2 {
		b.Skip("set SRQ_SHARD_ADDRS to 2+ redis addrs (e.g. 127.0.0.1:6379,127.0.0.1:6380)")
	}
	b.Run(fmt.Sprintf("proc_%d", len(addrs)), func(b *testing.B) {
		clients := clientsFromAddrs(b, addrs)
		cfg := shardCfgClients(ShardByGroup, clients)
		p := newShardedBenchProducer(b, cfg, "bench-shard-multiproc")
		parallelPublishByGroup(b, p, 8)
	})
}

// BenchmarkShardPublish_KeyFanout — много групп/ключей: 1 DB vs разнос по 4 DB.
// Нагружает dict и индексы; multi-DB может выиграть даже на одном процессе.
func BenchmarkShardPublish_KeyFanout(b *testing.B) {
	const groups = 512
	payload := []byte("bench")

	run := func(b *testing.B, producer *Producer) {
		ctx := context.Background()
		var seq atomic.Int64
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				n := seq.Add(1)
				g := fmt.Sprintf("g:%d", n%groups)
				_ = producer.Publish(ctx, &Task{
					ID:        fmt.Sprintf("f-%d", n),
					Groups:    []string{g},
					Partition: fmt.Sprintf("!p:%d", n%groups),
					Payload:   payload,
					Scheduled: time.Now(),
				})
			}
		})
	}

	b.Run("single_db", func(b *testing.B) {
		clients := clientsSameHostDBs(b, 12)
		run(b, NewProducer(clients[0], "bench-fanout-1"))
	})

	b.Run("multi_db_4", func(b *testing.B) {
		clients := clientsSameHostDBs(b, 12, 13, 14, 15)
		cfg := shardCfgClients(ShardByGroup, clients)
		run(b, newShardedBenchProducer(b, cfg, "bench-fanout-4"))
	})
}

// BenchmarkShardConsume_Parallel — seed b.N задач, drain пулом из 8 воркеров.
func BenchmarkShardConsume_Parallel(b *testing.B) {
	const workers = 8

	seed := func(b *testing.B, producer *Producer, n int) {
		ctx := context.Background()
		const batchSize = 100
		for i := 0; i < n; i += batchSize {
			count := batchSize
			if i+count > n {
				count = n - i
			}
			tasks := make([]*Task, count)
			for j := 0; j < count; j++ {
				k := i + j
				tasks[j] = &Task{
					ID:        fmt.Sprintf("c-%d", k),
					Groups:    []string{fmt.Sprintf("acct:%d", k%64)},
					Payload:   []byte("x"),
					Scheduled: time.Now().Add(-time.Second),
				}
			}
			if err := producer.Publish(ctx, tasks...); err != nil {
				b.Fatalf("seed: %v", err)
			}
		}
	}

	runDrain := func(b *testing.B, pool *ConsumerPool) {
		pool.SetCount(workers)
		pool.SetPollInterval(5 * time.Millisecond)
		pool.SetPrefetchCount(10)

		var done atomic.Int64
		target := int64(b.N)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.Consume(ctx, func(task *Task) error {
				if done.Add(1) >= target {
					cancel()
				}
				return nil
			})
		}()
		wg.Wait()
	}

	b.Run("single_db", func(b *testing.B) {
		clients := clientsSameHostDBs(b, 12)
		p := NewProducer(clients[0], "bench-c-1")
		seed(b, p, b.N)
		pool := NewConsumerPool(clients[0], "bench-c-1")
		runDrain(b, pool)
	})

	b.Run("multi_db_4", func(b *testing.B) {
		clients := clientsSameHostDBs(b, 12, 13, 14, 15)
		cfg := shardCfgClients(ShardByGroup, clients)
		p := newShardedBenchProducer(b, cfg, "bench-c-4")
		seed(b, p, b.N)
		pool := NewConsumerPool(nil, "bench-c-4")
		if err := pool.SetShards(cfg); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = pool.Close() })
		runDrain(b, pool)
	})
}
