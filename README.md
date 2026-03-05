# Smart Redis Queue

Очередь задач на Redis с поддержкой партиций, приоритетов, отложенного выполнения и строгих гарантий порядка.

## Возможности

- **Гарантия порядка выполнения** — ordered-партиции (префикс `!`) обрабатываются строго последовательно одним консьюмером; при reject порядок сохраняется
- **Отложенные сообщения** — задачи с полем `Scheduled` становятся доступны только после указанного времени
- **Чистка мёртвых консьюмеров** — heartbeat и автоматическое возвращение задач при падении воркера
- **Приоритет выполнения** — в рамках партиции задачи с большим `Priority` обрабатываются первыми
- **Батч-добавление** — атомарная публикация нескольких задач за один вызов `Publish`
- **Prefetch с сохранением порядка** — для ordered-партиций при reject первой задачи в батче остальные тоже reject'ятся и возвращаются в правильном порядке
- **Rate limiting** — `RejectWithDelay` для ordered-партиций ставит TTL-блок, партиция не берётся до истечения задержки
- **Идемпотентность** — добавление по `ID` (NX), дубликаты отклоняются

## Требования

- Go 1.22+
- Redis 6+

## Установка

```bash
go get github.com/Rinsvent/smart-redis-queue
```

## Быстрый старт

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/Rinsvent/smart-redis-queue"
)

func main() {
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    defer rdb.Close()

    ctx := context.Background()
    producer := redisqueue.NewProducer(rdb, "my-queue")
    consumer := redisqueue.NewConsumer(rdb, "my-queue", "")
    defer consumer.Close()

    // Публикуем задачу
    err := producer.Publish(ctx, &redisqueue.Task{
        ID:        "task-1",
        Payload:   []byte(`{"action": "send_email"}`),
        Scheduled: time.Now(),
    })
    if err != nil {
        log.Fatal(err)
    }

    // Обрабатываем
    consumer.Consume(ctx, func(task *redisqueue.Task) error {
        log.Printf("Обработано: %s, payload: %s", task.ID, string(task.Payload))
        return nil // nil = Ack, ошибка = Reject
    })
}
```

## Примеры

### Отложенные сообщения

```go
// Задача станет доступна через 5 минут
producer.Publish(ctx, &redisqueue.Task{
    ID:        "delayed-task",
    Payload:   []byte("data"),
    Scheduled: time.Now().Add(5 * time.Minute),
})
```

### Приоритеты

```go
// Задачи с большим Priority обрабатываются первыми
producer.Publish(ctx,
    &redisqueue.Task{ID: "low", Partition: "p1", Priority: 1, Payload: []byte("low"), Scheduled: time.Now()},
    &redisqueue.Task{ID: "high", Partition: "p1", Priority: 10, Payload: []byte("high"), Scheduled: time.Now()},
)
// Порядок: high, low
```

### Ordered-партиции (гарантия порядка)

```go
// Партиция с префиксом "!" — только один консьюмер, порядок строго сохраняется
producer.Publish(ctx,
    &redisqueue.Task{ID: "1", Partition: "!user-123", Payload: []byte("a"), Scheduled: time.Now()},
    &redisqueue.Task{ID: "2", Partition: "!user-123", Payload: []byte("b"), Scheduled: time.Now()},
)
// Всегда обработаются по порядку: 1, 2
```

### Батч-добавление

```go
tasks := make([]*redisqueue.Task, 100)
for i := range tasks {
    tasks[i] = &redisqueue.Task{
        ID:        fmt.Sprintf("task-%d", i),
        Payload:   []byte(fmt.Sprintf("payload-%d", i)),
        Scheduled: time.Now(),
    }
}
err := producer.Publish(ctx, tasks...)
// Атомарно: либо все добавлены, либо ошибка (в т.ч. при дубликатах)
```

### Reject с задержкой (rate limit)

```go
consumer.Consume(ctx, func(task *redisqueue.Task) error {
    if rateLimited {
        // Партиция ! не будет браться 60 секунд
        return redisqueue.NewRejectWithDelay(errors.New("rate limit"), 60)
    }
    return nil
})
```

### Пул консьюмеров

```go
pool := redisqueue.NewConsumerPool(rdb, "my-queue")
pool.SetCount(5)
pool.SetPrefetchCount(10)
pool.SetPollInterval(500 * time.Millisecond)

pool.Consume(ctx, func(task *redisqueue.Task) error {
    return process(task)
})
```

### Ручное управление (Get / Ack / Reject)

```go
ch := consumer.GetChan(ctx)
for task := range ch {
    if err := handle(task); err != nil {
        consumer.Reject(ctx, task.ID, 0) // вернуть в очередь
    } else {
        consumer.Ack(ctx, task.ID)
    }
}
```

## Конфигурация консьюмера

```go
consumer.SetPollInterval(2 * time.Second) // интервал при пустой очереди
consumer.SetPrefetchCount(10)             // задач за один Get (по умолчанию 5)
```

## Docker

```bash
docker compose up -d
# Redis на localhost:6379
```

## Запуск примера

```bash
# Redis должен быть запущен (docker compose up -d)
go run ./examples/basic
```

## CLI Manager

Утилита для обслуживания очередей:

```bash
make manager
./bin/manager --help
```

### Команды

| Команда | Описание |
|---------|----------|
| `info` (alias: list, ls) | Информация об очередях: партиции, pending, in-progress, консьюмеры |
| `purge` | Очистить очередь (или только указанную партицию) |
| `retry` | Вернуть in-progress задачи в очередь (для зависших после падения консьюмера) |

### Примеры

```bash
# Все очереди
./bin/manager info

# Конкретная очередь
./bin/manager info -q my-queue

# С фильтром по партиции
./bin/manager info -q my-queue -p "!user-123"

# Очистить очередь
./bin/manager purge -q my-queue

# Очистить только партицию
./bin/manager purge -q my-queue -p base

# Вернуть зависшие задачи
./bin/manager retry -q my-queue
```

Глобальные флаги: `-a` (addr), `-P` (password), `--db`. Переменные: `REDIS_ADDR`, `REDIS_PASSWORD`.

## Тесты

```bash
# Требуется Redis на localhost:6379
make test

# Короткие тесты (без long-running)
make test-short

# Покрытие
make test-coverage
```

## API

| Тип | Описание |
|-----|----------|
| `Producer` | Публикация задач |
| `Consumer` | Один консьюмер (Get/Ack/Reject, Consume, GetChan) |
| `ConsumerPool` | Пул консьюмеров |
| `Admin` | Обслуживание: Inspect, Purge, Retry |
| `Task` | Задача: ID, Partition, Priority, Payload, Scheduled |
| `RejectWithDelay` | Ошибка для отложенного reject (rate limit) |

## Ключи Redis

Очередь использует префикс `queue:{queueName}:`:

- `queue:{name}:partitions` — множество партиций
- `queue:{name}:partition:{code}:{priority}` — ZSET задач по партиции и приоритету
- `queue:{name}:consumers` — множество консьюмеров
- `queue:{name}:consumer:{id}` — heartbeat консьюмера (TTL 120 сек)

## Contributing

Приветствуются pull request'ы! Подробнее: [CONTRIBUTING.md](CONTRIBUTING.md)

Перед отправкой:

1. Запустите `make test`
2. Добавьте тесты для новой функциональности
3. Соблюдайте стиль кода (gofmt)

### Как внести вклад

- 🐛 [Сообщить об ошибке](https://github.com/Rinsvent/smart-redis-queue/issues/new)
- 💡 [Предложить улучшение](https://github.com/Rinsvent/smart-redis-queue/issues/new)
- 📖 Улучшить документацию
- 🔧 Исправить баг или добавить фичу — fork → branch → PR

## Лицензия

MIT — см. [LICENSE](LICENSE).
