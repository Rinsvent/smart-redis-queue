package gateway

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
)

func setupGWBench(b *testing.B) (*Gateway, *redis.Client) {
	b.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: gatewayTestDB})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		b.Skipf("Redis not available: %v", err)
	}
	_ = rdb.FlushDB(ctx)
	gw, err := New(Config{Redis: rdb, SkipPersistCheck: true, LossWindowOverride: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	if err := gw.Boot(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { gw.Close(); _ = rdb.Close() })
	return gw, rdb
}

func BenchmarkGatewayVsDirect_Publish(b *testing.B) {
	sizes := []int{256, 16 * 1024}
	for _, size := range sizes {
		payload := bytes.Repeat([]byte("x"), size)
		b.Run(fmt.Sprintf("direct_%dB", size), func(b *testing.B) {
			rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: gatewayTestDB})
			ctx := context.Background()
			if err := rdb.Ping(ctx).Err(); err != nil {
				b.Skip(err)
			}
			_ = rdb.FlushDB(ctx)
			p := redisqueue.NewProducer(rdb, "bench-gw")
			b.Cleanup(func() { _ = rdb.Close() })
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = p.Publish(ctx, &redisqueue.Task{
					ID: fmt.Sprintf("d-%d", i), Payload: payload, Scheduled: time.Now(),
				})
			}
		})
		b.Run(fmt.Sprintf("gateway_%dB", size), func(b *testing.B) {
			gw, _ := setupGWBench(b)
			ctx := context.Background()
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = gw.PublishTasks(ctx, "bench-gw", []*redisqueue.Task{{
					ID: fmt.Sprintf("g-%d", i), Payload: payload, Scheduled: time.Now(),
				}}, nil)
			}
		})
	}
}

func BenchmarkGatewayVsDirect_FullCycle(b *testing.B) {
	sizes := []int{256, 16 * 1024}
	for _, size := range sizes {
		payload := bytes.Repeat([]byte("x"), size)
		b.Run(fmt.Sprintf("direct_%dB", size), func(b *testing.B) {
			rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: gatewayTestDB})
			ctx := context.Background()
			if err := rdb.Ping(ctx).Err(); err != nil {
				b.Skip(err)
			}
			_ = rdb.FlushDB(ctx)
			p := redisqueue.NewProducer(rdb, "bench-gw-c")
			c := redisqueue.NewConsumer(rdb, "bench-gw-c", "bench-c", redisqueue.WithPrefetchCount(10))
			b.Cleanup(func() { c.Close(); _ = rdb.Close() })
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := fmt.Sprintf("d-%d", i)
				_ = p.Publish(ctx, &redisqueue.Task{
					ID: id, Payload: payload, Scheduled: time.Now().Add(-time.Second),
				})
				for {
					tasks, _ := c.Get(ctx)
					if len(tasks) == 0 {
						continue
					}
					_ = c.Ack(ctx, tasks[0].ID)
					break
				}
			}
		})
		b.Run(fmt.Sprintf("gateway_%dB", size), func(b *testing.B) {
			gw, _ := setupGWBench(b)
			ctx := context.Background()
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := fmt.Sprintf("g-%d", i)
				_ = gw.PublishTasks(ctx, "bench-gw-c", []*redisqueue.Task{{
					ID: id, Payload: payload, Scheduled: time.Now().Add(-time.Second),
				}}, nil)
				for {
					tasks, _ := gw.GetTasks(ctx, "bench-gw-c", "bench-c", 10)
					if len(tasks) == 0 {
						continue
					}
					_ = gw.AckTask(ctx, "bench-gw-c", "bench-c", tasks[0].ID, nil)
					break
				}
			}
		})
	}
}
