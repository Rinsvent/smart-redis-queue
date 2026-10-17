# Smart Redis Queue

[![CI](https://github.com/Rinsvent/smart-redis-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Rinsvent/smart-redis-queue/actions/workflows/ci.yml)
[![coverage](https://raw.githubusercontent.com/Rinsvent/smart-redis-queue/main/.github/badges/coverage.svg)](https://github.com/Rinsvent/smart-redis-queue/actions/workflows/ci.yml)

Очередь задач на Redis с поддержкой партиций, приоритетов, отложенного выполнения и строгих гарантий порядка.

## Возможности

- **Гарантия порядка выполнения** — ordered-партиции (префикс `!`) обрабатываются строго последовательно одним консьюмером; reject и reclaim мёртвого consumer возвращают задачу с исходным ZSET score (без `priority+1`)
- **Отложенные сообщения** — задачи с полем `Scheduled` становятся доступны только после указанного времени
- **Чистка мёртвых консьюмеров** — heartbeat и автоматическое возвращение задач при падении воркера
- **Приоритет выполнения** — в рамках партиции задачи с большим `Priority` обрабатываются первыми
- **Батч-добавление** — атомарная публикация нескольких задач за один вызов `Publish`
- **Prefetch с сохранением порядка** — для ordered-партиций при reject первой задачи в батче остальные тоже reject'ятся и возвращаются в правильном порядке
- **Rate limiting** — `RejectWithDelay` / `BlockGroups`: группы через score в `groups`, партиции через `blocked` (`p:`); Get обходит только свободные группы
- **Группы** — `Task.Groups` для rate-limit scope (напр. коннектор); без групп → `_default`
- **Идемпотентность** — Publish по `ID` (`SET NX`); опционально после Ack окно dedupe: тот же ID снова не примется, пока жив TTL на `payload`
- **Debounce на Publish** — id с префиксом `deb!` (`DebounceID`): всегда последний payload; in-flight → флаг и reactivate на Ack
- **Concurrency tags** — теги `race!…`: взаимное исключение ресурса между партициями (opt-in `SetConcurrencyTags`)
- **Теги** — индексация задач для подсчёта и массового удаления (`Admin.CountByTag` / `RemoveByTag`)
- **Backpressure на Publish** — opt-in лимит глубины по тегу (`WithTagBackpressure` / `WithTagLimits`); при превышении весь батч не пишется
- **Rate limit на Get** — opt-in слои group (токен/коннектор) + partition (чат); deny → block соответствующего scope без выдачи задачи
- **Удаление без consumer** — `Admin.Remove` снимает ожидающую задачу по ID

## Требования

- Go 1.25+ (см. `go.mod`; desktop UI тянет Wails / `golang.org/x`)
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

### Backpressure по тегу (глубина очереди)

Продюсер не заливает задачи, пока по тегу уже «полный» буфер (`SCARD tag` + размер батча > limit). Проверка в начале add-Lua: при отказе **ни одна** задача из вызова не добавляется → `ErrBackpressure`.

Рекомендация: один стабильный тег потока на задачу (например `tg` или `stats:day`) — так проще квотировать. Несколько лимитов — AND (срабатывает первый превышенный).

```go
// один тег
err := producer.PublishWith(ctx, []redisqueue.PublishOption{
    redisqueue.WithTagBackpressure("tg", 100),
}, &redisqueue.Task{
    ID: "m1", Tags: []string{"tg"}, Payload: []byte(`{}`), Scheduled: time.Now(),
})

// несколько
err = producer.PublishWith(ctx, []redisqueue.PublishOption{
    redisqueue.WithTagLimits(map[string]int64{"tg": 100, "vk:123": 200}),
}, tasks...)

var bp *redisqueue.ErrBackpressure
if errors.As(err, &bp) {
    // подождать / уменьшить батч; bp.Tag, bp.Count, bp.Limit
}
```

Без опций `Publish` — прежний путь (без `SCARD`). Не путать с rate limit по времени и с idempotency TTL по ID.

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

#### Транспортный rate limit на Get (opt-in)

Чтобы не делать Get → внешний Allow → Reject, лимиты вшиваются в Get (алгоритмы как в [rate-limiter](https://github.com/Rinsvent/rate-limiter)). Два слоя AND:

- **Group** — квота коннектора/токена по **текущей** группе обхода Get; deny → блок этой группы на `RetryAfter`
- **Partition** — квота чата/юзера; deny → `blocked` только этой партиции

`Algorithm` (пусто = `token-bucket`): `token-bucket`, `leaky-bucket`, `fixed-window-counter`, `sliding-window-log`, `sliding-window-counter`.

```go
_ = consumer.SetRateLimit(&redisqueue.RateLimitConfig{
    Algorithm: redisqueue.RateLimitTokenBucket,
    Group: &redisqueue.RateLimitLayer{
        Rates: []redisqueue.Rate{{Duration: time.Second, Value: 30}},
    },
    Partition: &redisqueue.RateLimitLayer{
        Rates: []redisqueue.Rate{{Duration: time.Second, Value: 1}, {Duration: time.Minute, Value: 20}},
    },
})
```

Любой слой можно опустить (`nil`). Без `SetRateLimit` — прежний Get без ключей `queue:{q}:rl:*`.  
Адаптеры tg/vk — обычно разные очереди/consumer с разными Rates. Не путать с backpressure по тегу (F3) и с ручным `RejectWithDelay` в handler.

Группы: `queue:{name}:groups` (ZSET) — score `≤ now` = свободна, `> now` = блок; свободные = `ZRANGEBYSCORE -inf now`.  
Партиции (ordered Reject): `queue:{name}:blocked` — member=partition → unlockAt.  
Ready группы обходится через `ZRANGE` с random index (не грузит миллионы партиций в Lua разом).  
Нет свободных групп → Get пуст (обход `partitions` без groups не делается).

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
        // idempotencyTtl>0: после успеха payload:{id} остаётся на TTL — повторный Publish с тем же ID отклоняется (NX)
        consumer.Ack(ctx, task.ID, 2*time.Second)
    }
}
```

То же для `Consume` / пула: `consumer.SetIdempotencyTtl(2 * time.Second)`.

### Idempotency TTL после Ack (не rate limit)

После успешного Ack при `idempotencyTtl > 0` ключ `payload:{id}` не удаляется, а ставится маркер + `EXPIRE`. Пока TTL жив, `Publish` с тем же `ID` не проходит (`SET NX`).

Это **не throttling и не rate limit** в обычном смысле:

| | Idempotency TTL | Rate limit / throttle |
|--|-----------------|------------------------|
| Что ограничивает | Повтор **того же** `ID` | N разных событий за окно |
| Когда стартует окно | После **успешного** Ack | Обычно на каждый attempt / по времени |
| Новый payload с тем же ID в окне | Молча не принимается | Зависит от политики |
| Разные ID | Все проходят | Считаются в лимит |

Ближе к названиям: *dedupe window* / *cooldown по task ID после успеха*.

**Когда хватает:** стабильный бизнес-ключ в `ID` (например `stats:2026-10-15`) + при необходимости ordered-партиция — «успешный прогон этого ключа не чаще чем раз в TTL». Повторные Publish с тем же ID в окне просто no-op.

**Когда не хватает:** нужны обновления payload в полёте / всегда **последний** снимок → [Debounce](#debounce-publish-префикс-deb). Лимит скорости / глубины → rate limit в Get и backpressure по тегу, не idempotency TTL.

### Debounce Publish (префикс `deb!`)

Если `Task.ID` начинается с `redisqueue.DebouncePrefix` (`deb!`, хелпер `DebounceID`), Publish **не** отклоняется при занятом id:

| Состояние | Поведение |
|-----------|-----------|
| Нет задачи | Обычный enqueue |
| В очереди (waiting) | Перезапись payload + сдвиг `Scheduled` |
| In-flight | Перезапись payload + флаг `queue:{q}:debounce:{id}` (until); после **Ack** задача снова в ZSET с этим score и актуальным payload (без F1-маркера на этом Ack) |

```go
id := redisqueue.DebounceID("stats:2026-10-15")
_ = producer.Publish(ctx, &redisqueue.Task{
    ID: id, Payload: payload, Scheduled: time.Now().Add(time.Second),
})
```

Без префикса — прежний `SET NX` / `ErrTasksAlreadyExist`. Не путать с idempotency TTL (окно после успеха) и с rate limit.

### Concurrency tags (`race!`)

Если в `Task.Tags` есть теги с префиксом `race!` (`RaceTagPrefix`), Publish кладёт их в `race:tags:{id}`. Проверка и HASH-локи `race:locks` — только при `consumer.SetConcurrencyTags(true)`:

```go
consumer.SetConcurrencyTags(true)
_ = producer.Publish(ctx, &redisqueue.Task{
    ID: "msg-1", Partition: "!chat:1",
    Tags: []string{redisqueue.RaceTagPrefix + "file:" + fileHash},
    Payload: payload,
})
```

Пока задача in-flight держит лок, другая партиция с тем же `race!` тегом не выдаётся. Ack/Reject снимают лок (при включённой опции). Без `SetConcurrencyTags` — нулевой оверхед Get (индекс на Publish всё равно пишется).

Не путать с `RejectWithDelay` / `BlockGroups`: это парковка group/partition после обработки, а не запрет Publish по ID.

### Метрики очереди (opt-in)

По умолчанию **выключены** — hot path без оверхеда. Включаются на воркере:

```go
consumer.SetStatsRetention(time.Hour)          // g/a/r раз в секунду в Redis
consumer.SetTagsStats("all", "campaign:spring") // опционально: SCARD этих тегов раз в ~1s

// то же на пуле
pool.SetStatsRetention(time.Hour)
pool.SetTagsStats("all")
```

- **get / ack / reject** — аккумулятор в Go (привязка к unix-секунде события), flush `HINCRBY` в `queue:{q}:stats:{sec}`; несколько хостов суммируются.
- **TagsStats** — отдельная goroutine (~1s, как ping): только `SCARD queue:{q}:tag:{tag}` для указанных тегов → поля `t:{tag}`. Полный обход групп/партиций не делается.
- Чтобы видеть «total», тегируйте задачи (например `all`) и добавьте тег в `SetTagsStats` — выбор скорость vs статистика за пользователем.
- Ключи: `queue:{q}:stats:{unixSec}` (HASH, TTL=retention), `queue:{q}:stats:index` (ZSET).
- Desktop: график (get/ack/reject + теги из Redis); плюс **Watch** у тега в UI — локальный poll `CountByTag` раз в 5s в `localStorage`, без записи в Redis.

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
| `desktop` | GUI manager (Wails); `-tags wails,production,webkit2_41` (Ubuntu 24.04+) |

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

### Desktop UI (Wails)

GUI для Redis-хостов (без выбора DB в профиле): обзор очередей по всем DB, режимы Partitions / Groups / Tags, purge, block/unblock партиций (rate-limit ZSET `blocked`), retry, payload по ID и операции по тегам.

Сборка GUI вынесена за build-тег `wails`, чтобы обычный `go build ./...` и CI не требовали CGO/GTK.

Профиль: `name` + `addr` + `password`. Каталог очередей строится SCAN’ом `queue:*:partitions` и `queue:*:consumers` по DB `0..N-1` (N из `CONFIG GET databases`, иначе 16). Обновление каталога — раз в 5 минут и кнопкой Refresh. Одноимённые очереди на разных DB схлопываются в списке; в карточке — табы по DB.

#### Готовые бинарники (GitHub Releases)

На тег `v*` workflow собирает:

| Asset | Платформа |
| --- | --- |
| `smart-redis-queue-desktop-linux-amd64` | Linux (Ubuntu 24.04+, webkit2gtk-4.1) |
| `smart-redis-queue-desktop-windows-amd64.exe` | Windows 10 / 11 (WebView2) |

Linux: при GBM/EGL abort на NVIDIA/Wayland запускайте с `WEBKIT_DISABLE_DMABUF_RENDERER=1` (при необходимости ещё `GDK_BACKEND=x11`).

Windows: нужен [WebView2 Runtime](https://developer.microsoft.com/microsoft-edge/webview2/) (обычно уже есть на Win10/11). Windows 8 не поддерживается.

#### Сборка из исходников

Требования:

- Go 1.25+, CGO, зависимости Wails для вашей ОС ([дока Wails](https://wails.io/docs/gettingstarted/installation))
- Node.js 18+ (для frontend; Vite 6)
- Redis для проверки подключений из UI

```bash
# CLI Wails (один раз)
go install github.com/wailsapp/wails/v2/cmd/wails@latest

# Frontend
cd internal/desktopui/frontend && npm install && npm run build && cd -

# Запуск GUI
# wails — наш тег; production — тег Wails; webkit2_41 — Ubuntu 24.04+ (webkit2gtk-4.1)
# WEBKIT_DISABLE_DMABUF_RENDERER — обход GBM EGL abort на NVIDIA/Wayland
WEBKIT_DISABLE_DMABUF_RENDERER=1 go run -tags wails,production,webkit2_41 ./cmd/manager desktop
```

Если всё ещё падает на Wayland: `GDK_BACKEND=x11 WEBKIT_DISABLE_DMABUF_RENDERER=1 go run ...`

На Ubuntu 22.04 / системах с `libwebkit2gtk-4.0-dev` тег `webkit2_41` не нужен.

Зависимости ОС (Ubuntu 24.04):

```bash
sudo apt install libgtk-3-dev libwebkit2gtk-4.1-dev
```

Профили сохраняются в `~/.config/smart-redis-queue/connections.json` (или `os.UserConfigDir()`). Поле `db` в старых профилях игнорируется.

Без `-tags wails` команда `desktop` возвращает понятную ошибку — это ожидаемо.

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
- `queue:{name}:consumer:{id}:tasks` — in-flight: `taskId → originalScore`
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

**Группы / `blocked`:**

1. Выкатить consumer’ов с Get по `groups`/`ready` (без свободных групп Get пуст).
2. Продюсеры с `Task.Groups` (без Groups → `_default`).
3. Включать `BlockGroups` / `RejectWithDelay(..., groups)`.

Не смешивать «новый Reject + старый Get» — старый Get не видит блоки групп/партиций.

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
