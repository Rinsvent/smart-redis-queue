# Быстрый старт

## 1. Redis

```bash
docker compose up -d
# Redis на localhost:6379
```

## 2. Библиотека

```bash
go get github.com/Rinsvent/smart-redis-queue
```

## 3. Hello World

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/redis/go-redis/v9"
    redisqueue "github.com/Rinsvent/smart-redis-queue"
)

func main() {
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    defer rdb.Close()

    ctx := context.Background()
    producer := redisqueue.NewProducer(rdb, "my-queue")
    consumer := redisqueue.NewConsumer(rdb, "my-queue", "")
    defer consumer.Close()

    err := producer.Publish(ctx, &redisqueue.Task{
        ID:        "task-1",
        Payload:   []byte(`{"action":"send_email"}`),
        Scheduled: time.Now(),
    })
    if err != nil {
        log.Fatal(err)
    }

    consumer.Consume(ctx, func(task *redisqueue.Task) error {
        log.Printf("done: %s %s", task.ID, string(task.Payload))
        return nil // nil = Ack, error = Reject
    })
}
```

Готовый пример в репозитории:

```bash
go run ./examples/basic
```

## 4. Внешний API с лимитами

Публикуем задачу с порядком на ресурс и группой на аккаунт:

```go
err := producer.Publish(ctx, &redisqueue.Task{
    ID:        "job-42",
    Partition: "!acct_9f3a:order_8841",
    Groups:    []string{"acct:9f3a"},
    Payload:   []byte(`{"op":"sync"}`),
    Scheduled: time.Now(),
})
```

На consumer можно повесить квоты прямо на Get — без схемы «взяли задачу → сходили в Allow → вернули назад»:

```go
_ = consumer.SetRateLimit(&redisqueue.RateLimitConfig{
    Algorithm: redisqueue.RateLimitTokenBucket,
    Group: &redisqueue.RateLimitLayer{
        Rates: []redisqueue.Rate{{Duration: time.Second, Value: 30}},
    },
    Partition: &redisqueue.RateLimitLayer{
        Rates: []redisqueue.Rate{
            {Duration: time.Second, Value: 1},
            {Duration: time.Minute, Value: 20},
        },
    },
})
```

Если провайдер всё же ответил 429 — паркуем группу:

```go
return redisqueue.NewRejectWithDelay(err, 2.0, "acct:9f3a")
```

Разбор: [Группы и rate limit](features/rate-limit.md).

## 5. Ops без кода

```bash
make manager
./bin/manager info
./bin/manager info -q my-queue
```

Web UI (тот же интерфейс, что у Desktop, без CGO):

```bash
make server
./bin/manager-server server --host 127.0.0.1 --port 34115
```

Desktop и мобильные сборки — [Приложения и сервер](apps.md).

## Дальше

- [Фичи](features/index.md)
- [Гарантии доставки](delivery.md)
- [Сравнение с Kafka / Rabbit / NATS](comparison.md)
