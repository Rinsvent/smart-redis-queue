package journal

import (
	"context"
	"fmt"
	"testing"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
)

func BenchmarkBridge_Pump(b *testing.B) {
	rdb := testRedis(b)
	ctx := context.Background()
	src := NewMemSource()
	base := time.Now().Add(-time.Minute)
	for i := 0; i < b.N+64; i++ {
		src.Insert(&redisqueue.Task{
			ID:        fmt.Sprintf("b%08d", i),
			Payload:   []byte("bench"),
			Scheduled: base.Add(time.Duration(i) * time.Microsecond),
		})
	}
	br := NewBridge(rdb, "jb", "qb", src)
	b.ReportAllocs()
	b.ResetTimer()
	left := b.N
	for left > 0 {
		n, err := br.Pump(ctx, 64)
		if err != nil {
			b.Fatal(err)
		}
		if n == 0 {
			b.Fatalf("source exhausted, left=%d", left)
		}
		left -= n
	}
}

func BenchmarkBridge_PumpGetAck(b *testing.B) {
	rdb := testRedis(b)
	ctx := context.Background()
	src := NewMemSource()
	base := time.Now().Add(-time.Minute)
	for i := 0; i < b.N+64; i++ {
		src.Insert(&redisqueue.Task{
			ID:        fmt.Sprintf("c%08d", i),
			Payload:   []byte("bench"),
			Scheduled: base.Add(time.Duration(i) * time.Microsecond),
		})
	}
	br := NewBridge(rdb, "jba", "qba", src)
	c := redisqueue.NewConsumer(rdb, "qba", "cba")
	defer c.Close()
	c.SetPrefetchCount(64)
	c.SetJournalAck(func(ctx context.Context, taskID string) error {
		return src.Ack(ctx, taskID, nil)
	})
	b.ReportAllocs()
	b.ResetTimer()
	acked := 0
	empty := 0
	for acked < b.N {
		if _, err := br.Pump(ctx, 64); err != nil {
			b.Fatal(err)
		}
		tasks, err := c.Get(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if len(tasks) == 0 {
			empty++
			if empty > 10000 {
				b.Fatalf("Get empty after %d acks", acked)
			}
			continue
		}
		empty = 0
		for _, t := range tasks {
			if err := c.Ack(ctx, t.ID); err != nil {
				b.Fatal(err)
			}
			acked++
			if acked >= b.N {
				return
			}
		}
	}
}
