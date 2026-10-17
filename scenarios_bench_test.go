package redisqueue

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

// Сценарные бенчи (payload / compression / ordered). Запуск:
//
//	go test -run='^$' -bench='BenchmarkPayload|BenchmarkCompress|BenchmarkOrdered' -benchtime=1s -count=3 .
//
// Результаты и методология: docs/ANALYSIS.md

func compressiblePayload(size int) []byte {
	// Хорошо жмётся zstd (повторяющийся текст / JSON-подобное).
	pattern := []byte(`{"event":"msg","text":"hello-world-chunk-"}`)
	out := make([]byte, 0, size)
	for len(out) < size {
		out = append(out, pattern...)
	}
	return out[:size]
}

func BenchmarkPayloadPublish(b *testing.B) {
	sizes := []int{128, 1024, 16 * 1024, 64 * 1024, 256 * 1024}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("raw_%dB", size), func(b *testing.B) {
			producer, _, _ := setupBenchQueue(b)
			ctx := context.Background()
			payload := bytes.Repeat([]byte("x"), size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = producer.Publish(ctx, &Task{
					ID: fmt.Sprintf("p-%d", i), Payload: payload, Scheduled: time.Now(),
				})
			}
		})
	}
}

func BenchmarkPayloadFullCycle(b *testing.B) {
	sizes := []int{128, 1024, 16 * 1024, 64 * 1024}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("raw_%dB", size), func(b *testing.B) {
			producer, consumer, _ := setupBenchQueue(b)
			consumer.SetPrefetchCount(10)
			ctx := context.Background()
			payload := bytes.Repeat([]byte("x"), size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := fmt.Sprintf("c-%d", i)
				_ = producer.Publish(ctx, &Task{
					ID: id, Payload: payload, Scheduled: time.Now().Add(-time.Second),
				})
				for {
					tasks, _ := consumer.Get(ctx)
					if len(tasks) == 0 {
						continue
					}
					_ = consumer.Ack(ctx, tasks[0].ID, 0)
					break
				}
			}
		})
	}
}

func BenchmarkCompressPublish(b *testing.B) {
	sizes := []int{1024, 16 * 1024, 64 * 1024, 256 * 1024}
	for _, size := range sizes {
		for _, mode := range []string{"raw", "zstd"} {
			b.Run(fmt.Sprintf("%s_%dB", mode, size), func(b *testing.B) {
				producer, _, rdb := setupBenchQueue(b)
				if mode == "zstd" {
					producer.SetCompression(CompressionZstd)
					producer.SetCompressMinBytes(1)
				}
				ctx := context.Background()
				payload := compressiblePayload(size)
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = producer.Publish(ctx, &Task{
						ID: fmt.Sprintf("z-%d", i), Payload: payload, Scheduled: time.Now(),
					})
				}
				b.StopTimer()
				// Оценка RAM: MEMORY USAGE одного типичного ключа (последний id).
				if b.N > 0 {
					key := fmt.Sprintf("queue:bench-queue:payload:z-%d", b.N-1)
					n, err := rdb.Do(ctx, "MEMORY", "USAGE", key).Int64()
					if err == nil {
						b.ReportMetric(float64(n), "redis_B/op")
					}
				}
			})
		}
	}
}

func BenchmarkCompressFullCycle(b *testing.B) {
	sizes := []int{16 * 1024, 64 * 1024}
	for _, size := range sizes {
		for _, mode := range []string{"raw", "zstd"} {
			b.Run(fmt.Sprintf("%s_%dB", mode, size), func(b *testing.B) {
				producer, consumer, _ := setupBenchQueue(b)
				consumer.SetPrefetchCount(10)
				if mode == "zstd" {
					producer.SetCompression(CompressionZstd)
					producer.SetCompressMinBytes(1)
				}
				ctx := context.Background()
				payload := compressiblePayload(size)
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = producer.Publish(ctx, &Task{
						ID: fmt.Sprintf("zc-%d", i), Payload: payload, Scheduled: time.Now().Add(-time.Second),
					})
					for {
						tasks, _ := consumer.Get(ctx)
						if len(tasks) == 0 {
							continue
						}
						_ = consumer.Ack(ctx, tasks[0].ID, 0)
						break
					}
				}
			})
		}
	}
}

func BenchmarkOrderedPartitions(b *testing.B) {
	// Много ordered-партиций (как рассылка по чатам): Publish+drain.
	partCounts := []int{10, 100, 500}
	for _, parts := range partCounts {
		b.Run(fmt.Sprintf("parts-%d", parts), func(b *testing.B) {
			producer, consumer, _ := setupBenchQueue(b)
			consumer.SetPrefetchCount(5)
			ctx := context.Background()
			payload := []byte("x")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tasks := make([]*Task, parts)
				for j := 0; j < parts; j++ {
					tasks[j] = &Task{
						ID: fmt.Sprintf("o-%d-%d", i, j), Partition: fmt.Sprintf("!chat:%d", j),
						Payload: payload, Groups: []string{"tok"}, Scheduled: time.Now().Add(-time.Second),
					}
				}
				_ = producer.Publish(ctx, tasks...)
				b.StartTimer()
				consumed := 0
				for consumed < parts {
					got, err := consumer.Get(ctx)
					if err != nil {
						b.Fatal(err)
					}
					if len(got) == 0 {
						b.Fatal("empty Get before drain")
					}
					for _, t := range got {
						_ = consumer.Ack(ctx, t.ID, 0)
						consumed++
					}
				}
			}
		})
	}
}
