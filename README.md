# Smart Redis Queue

[![CI](https://github.com/Rinsvent/smart-redis-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Rinsvent/smart-redis-queue/actions/workflows/ci.yml)
[![coverage](https://raw.githubusercontent.com/Rinsvent/smart-redis-queue/main/.github/badges/coverage.svg)](https://github.com/Rinsvent/smart-redis-queue/actions/workflows/ci.yml)

Очередь задач на Redis с поддержкой партиций, приоритетов, отложенного выполнения и строгих гарантий порядка.

## Возможности

- **Гарантия порядка выполнения** — ordered-партиции (префикс `!`) обрабатываются строго последовательно одним консьюмером; при reject порядок сохраняется
- **Отложенные сообщения** — задачи с полем `Scheduled` становятся доступны только после указанного времени
- **Чистка мёртвых консьюмеров** — heartbeat и автоматическое возвращение задач при падении воркера
- **Приоритет выполнения** — в рамках партиции задачи с большим `Priority` обрабатываются первыми
- **Батч-добавление** — атомарная публикация нескольких задач за один вызов `Publish`
- **Prefetch с сохранением порядка** — для ordered-партиций при reject первой задачи в батче остальные тоже reject'ятся и возвращаются в правильном порядке
- **Rate limiting** — `RejectWithDelay` / `BlockGroups`: группы через score в `groups`, партиции через `blocked` (`p:`); Get обходит только свободные группы
- **Группы** — `Task.Groups` для rate-limit scope (напр. коннектор); без групп → `_default`
- **Идемпотентность** — добавление по `ID` (NX), дубликаты отклоняются
- **Теги** — индексация задач для подсчёта и массового удаления (`Admin.CountByTag` / `RemoveByTag`)
- **Удаление без consumer** — `Admin.Remove` снимает ожидающую задачу по ID

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

### Группы и rate limit

Группа — scope для парковки (например id коннектора). При Publish задаётся `Task.Groups`; пустые → `_default`.

```go
producer.Publish(ctx, &redisqueue.Task{
    ID:        "msg-1",
    Partition: "!163865653:chat-42",
    Groups:    []string{"conn:163865653"}, // одна группа на партицию — оптимально
    Payload:   []byte(`{}`),
    Scheduled: time.Now(),
})

consumer.Consume(ctx, func(task *redisqueue.Task) error {
    if connectorRateLimited {
        // Паркует всю группу: Get не обходит её ready, пока не истечёт Delay.
        // Для ordered (!) дополнительно блокируется сама партиция в blocked.
        return redisqueue.NewRejectWithDelay(errors.New("rate limit"), 0.3, "conn:163865653")
    }
    return nil
})

// Без задачи — Consumer или Admin:
consumer.BlockGroups(ctx, 0.3, "conn:163865653")
admin.BlockGroups(ctx, "my-queue", 0.3, "conn:163865653")
```

Группы: `queue:{name}:groups` (ZSET) — score `≤ now` = свободна, `> now` = блок; свободные = `ZRANGEBYSCORE -inf now`.  
Партиции (ordered Reject): `queue:{name}:blocked` — member=partition → unlockAt.  
Ready группы обходится через `ZRANGE` с random index (не грузит миллионы партиций в Lua разом).  
На миграции: `consumer.SetLegacyPartitionsFallback(true)`, пока старые продюсеры без Groups.

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

Паника в `handler` (nil-deref и т.п.) **не роняет процесс**: перехватывается, превращается в `HandlerPanicError` и идёт по той же ветке, что обычная ошибка → `Reject`. Остальные консьюмеры пула продолжают работу.

### Middleware вокруг handler

`Use` оборачивает `handler(task)` (логи, Sentry, метрики). Первый `Use` — внешний слой: видит `HandlerPanicError` как обычный `error`.

```go
pool.Use(func(next redisqueue.HandlerFunc) redisqueue.HandlerFunc {
    return func(task *redisqueue.Task) error {
        err := next(task)
        var pe *redisqueue.HandlerPanicError
        if errors.As(err, &pe) {
            log.Printf("handler panic on %s: %v\n%s", task.ID, pe.Value, pe.Stack)
            // sentry.CaptureException(pe) ...
        } else if err != nil {
            log.Printf("handler error on %s: %v", task.ID, err)
        }
        return err // nil → Ack, error/panic → Reject
    }
})

pool.Consume(ctx, process)
```

То же API есть у `Consumer.Use`.

### Ручное управление (Get / Ack / Reject)

```go
ch := consumer.GetChan(ctx)
for task := range ch {
    if err := handle(task); err != nil {
        consumer.Reject(ctx, task.ID, 0) // вернуть в очередь
    } else {
        consumer.Ack(ctx, task.ID, 2*time.Second) // 2 секунды после ack не будем принимать задачи с таким же taskID
    }
}
```

### Теги

Теги задаются при публикации. Разделитель в Lua/Redis — `#` (`TagSeparator`); символ `#` внутри значения тега запрещён.

```go
producer.Publish(ctx, &redisqueue.Task{
    ID:      "msg-1",
    Payload: []byte(`{"to":"user@example.com"}`),
    Tags:    []string{"mailing:42", "campaign:spring"},
    Scheduled: time.Now(),
})

admin := redisqueue.NewAdmin(rdb)

n, _ := admin.CountByTag(ctx, "my-queue", "mailing:42")
// n == 1 (pending + in-progress; после remove/ack уже не считаются)

// Удаление одной ожидающей задачи без consumer
res, _ := admin.Remove(ctx, "my-queue", "msg-1")
// res.Status: "removed" | "inprogress" | "missing"

// Массовое удаление по тегу (батчами, чтобы не блокировать Redis Lua надолго)
for {
    batch, err := admin.RemoveByTag(ctx, "my-queue", "mailing:42", redisqueue.RemoveByTagOptions{
        Limit:         100, // default 100, max 1000
        ReturnPayload: true,
    })
    if err != nil || len(batch) == 0 {
        break
    }
    onlyInProgress := true
    for _, item := range batch {
        if item.Status == redisqueue.RemovalStatusRemoved {
            onlyInProgress = false
            // item.TaskID, item.Payload
        }
    }
    // Если в батче только inprogress — снимать больше нечего, остальное у воркеров
    if onlyInProgress {
        break
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

CI (GitHub Actions) на каждый push/PR в `main`: сборка, `go test -race`, coverage в Job Summary и badge `.github/badges/coverage.svg` (обновляется на `main` без внешних сервисов).

## API

| Тип | Описание |
|-----|----------|
| `Producer` | Публикация задач |
| `Consumer` | Один консьюмер (Get/Ack/Reject, Consume, GetChan) |
| `ConsumerPool` | Пул консьюмеров |
| `Admin` | Обслуживание: Inspect, Purge, Retry, Remove, RemoveByTag, CountByTag, BlockGroups |
| `Task` | Задача: ID, Partition, Priority, Payload, Scheduled, Tags, Groups |
| `RejectWithDelay` | Ошибка для отложенного reject (`Delay`, `BlockGroups`) |
| `HandlerPanicError` | Паника в handler/middleware → Reject (процесс жив) |
| `HandlerMiddleware` / `Use` | Обертка вокруг handler (логи, Sentry) |
| `Consumer.BlockGroups` | Парковка групп без задачи |

### Admin: удаление и теги

| Метод | Описание |
|-------|----------|
| `Remove(queue, taskID)` | Снять ожидающую задачу по ID. `inprogress` не трогает |
| `RemoveByTag(queue, tag, opts)` | Снять до `Limit` задач с тегом (default 100, max 1000) |
| `CountByTag(queue, tag)` | `SCARD` индекса тега |

Статусы: `removed`, `inprogress`, `missing` (`RemovalStatus*`).

## Ключи Redis

Очередь использует префикс `queue:{queueName}:`:

- `queue:{name}:partitions` — множество партиций
- `queue:{name}:partition:{code}:{priority}` — ZSET задач по партиции и приоритету
- `queue:{name}:consumers` — множество консьюмеров
- `queue:{name}:consumer:{id}` — heartbeat консьюмера (TTL 120 сек)
- `queue:{name}:tag:{tag}` — SET taskId по тегу
- `queue:{name}:tags:{taskId}` — SET тегов задачи (для точечной чистки)
- `queue:{name}:groups` — ZSET групп (`≤now` = свободна, `>now` = блок)
- `queue:{name}:group:{g}:ready` — ZSET партиций группы (score=0; обход ZRANGE с random index)
- `queue:{name}:partition:{p}:groups` — SET групп партиции
- `queue:{name}:blocked` — ZSET блоков партиций (member=partition → unlockAt ms)

## Обновление библиотеки (совместимость)

Lua-скрипты в Redis регистрируются по SHA содержимого. Новая версия библиотеки добавляет/меняет скрипты рядом со старыми: процесс со старой версией пакета продолжает вызывать свой SHA, процесс с новой — свой. Конфликта «двух версий одного скрипта» нет.

| Сценарий | Поведение |
|----------|-----------|
| Старые продюсеры + новые консьюмеры | OK: add без тегов, новый ack просто не находит индекс тегов |
| Новые продюсеры **без** `Tags` + старые консьюмеры | OK: контракт данных тот же |
| Новые продюсеры **с** `Tags` + старые консьюмеры | Работает, но старый `Ack` не чистит `tag:*` / `tags:*` → возможны «осиротевшие» id в индексе. `Remove` / `RemoveByTag` подчищают `missing` |
| `Admin.Remove` / `RemoveByTag` / `CountByTag` | Только в новой версии; на старых воркерах методов нет |
| Новый Reject (пишет `groups` score / `blocked`) + старый Get | Старый Get **не видит** новый блок → сначала обновить всех consumer’ов |

**Теги** — порядок выката:

1. Обновить консьюмеры (чтобы `Ack` чистил теги).
2. Обновить продюсеры / сервисы, которые вызывают `Admin`.
3. Включать публикацию с `Tags`.

**Группы / `blocked`** — без downtime:

1. Выкатить consumer’ов с новым Get; на время миграции: `SetLegacyPartitionsFallback(true)` (обход `partitions`, если `groups` ещё пуст).
2. Выкатить продюсеры с `Task.Groups` (без Groups → `_default`).
3. Когда все задачи идут через Groups: `SetLegacyPartitionsFallback(false)` (дефолт) — только `ZRANGEBYSCORE` свободных групп.
4. Включать `BlockGroups` / `RejectWithDelay(..., groups)`.
5. Флаг fallback удалим в следующем major.

С нуля: fallback не включать. Не смешивать «новый Reject + старый Get» — старый Get не видит блоки групп/партиций.

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
