// Journal end-to-end: MemSource → Bridge → Redis queue → Consumer + journal Ack.
//
//	go run ./examples/journal

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/Rinsvent/smart-redis-queue/journal"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := "localhost:6379"
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		addr = a
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 5})
	defer rdb.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = rdb.FlushDB(ctx)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	src := journal.NewMemSource()
	now := time.Now()
	src.Insert(
		&redisqueue.Task{ID: "j1", Payload: []byte(`{"n":1}`), Scheduled: now},
		&redisqueue.Task{ID: "j2", Payload: []byte(`{"n":2}`), Scheduled: now},
	)

	const queue = "example-journal"
	br := journal.NewBridge(rdb, "example", queue, src)
	go func() {
		if err := br.Loop(ctx, 16, 200*time.Millisecond); err != nil && ctx.Err() == nil {
			log.Printf("bridge: %v", err)
		}
	}()

	consumer := redisqueue.NewConsumer(rdb, queue, "ex-c1")
	defer consumer.Close()
	consumer.SetJournalAck(func(ctx context.Context, taskID string) error {
		log.Printf("journal ack %s", taskID)
		return src.Ack(ctx, taskID, nil)
	})
	consumer.SetEmergencyResume(true)
	consumer.SetPollInterval(200 * time.Millisecond)

	log.Println("consuming (ctrl+c to stop)…")
	consumer.Consume(ctx, func(task *redisqueue.Task) error {
		log.Printf("handled %s %s", task.ID, task.Payload)
		return nil
	})
}
