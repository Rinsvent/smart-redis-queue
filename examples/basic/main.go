package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := "localhost:6379"
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		addr = a
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("Shutting down...")
		cancel()
	}()

	producer := redisqueue.NewProducer(rdb, "example-queue")
	consumer := redisqueue.NewConsumer(rdb, "example-queue", "")
	defer consumer.Close()

	// Публикуем несколько задач
	tasks := []*redisqueue.Task{
		{ID: "task-1", Payload: []byte(`{"msg": "hello"}`), Scheduled: time.Now()},
		{ID: "task-2", Payload: []byte(`{"msg": "world"}`), Scheduled: time.Now()},
		{ID: "task-3", Payload: []byte(`{"msg": "!"}`), Scheduled: time.Now().Add(2 * time.Second)},
	}
	if err := producer.Publish(ctx, tasks...); err != nil {
		log.Fatal(err)
	}
	log.Println("Published 3 tasks")

	consumer.SetPollInterval(500 * time.Millisecond)
	consumer.Consume(ctx, func(task *redisqueue.Task) error {
		log.Printf("Processed: %s -> %s", task.ID, string(task.Payload))
		return nil
	})

	log.Println("Done")
}
