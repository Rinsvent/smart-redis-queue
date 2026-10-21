# Smart Redis Queue

[![CI](https://github.com/Rinsvent/smart-redis-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Rinsvent/smart-redis-queue/actions/workflows/ci.yml)
[![coverage](https://raw.githubusercontent.com/Rinsvent/smart-redis-queue/main/.github/badges/coverage.svg)](https://github.com/Rinsvent/smart-redis-queue/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue?logo=markdown)](https://rinsvent.github.io/smart-redis-queue/)

Очередь задач на Redis с поддержкой партиций, приоритетов, отложенного выполнения и строгих гарантий порядка.

**Документация:** [rinsvent.github.io/smart-redis-queue](https://rinsvent.github.io/smart-redis-queue/) — зачем SRQ, get started, фичи, сравнение с Kafka/Rabbit, масштабирование, гарантии, apps/server, бенчмарки, roadmap.

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
- **Сжатие payload** — opt-in zstd на Producer (`SetCompression`); Get/Admin декодируют по маркеру
- **Теги** — индексация задач для подсчёта и массового удаления (`Admin.CountByTag` / `RemoveByTag`)
- **Backpressure на Publish** — opt-in лимит глубины по тегу (`WithTagBackpressure` / `WithTagLimits`); при превышении весь батч не пишется
- **Rate limit на Get** — opt-in слои group (токен/коннектор) + partition (чат); deny → block соответствующего scope без выдачи задачи
- **Удаление без consumer** — `Admin.Remove` снимает ожидающую задачу по ID
- **Шардинг** — несколько standalone Redis (`ShardByGroup` / `ByManual` / …); см. [docs](https://rinsvent.github.io/smart-redis-queue/sharding/)
- **Step recovery** — checkpoint шагов handler (`NewSteps` / `Do`); см. [docs](https://rinsvent.github.io/smart-redis-queue/features/step-recovery/)
- **Headers** — мета между попытками (`SetHeader` / ошибки при Reject); см. [docs](https://rinsvent.github.io/smart-redis-queue/features/headers/)

## Требования

- Go 1.25+ (см. `go.mod`; desktop/mobile UI — Wails v3)
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

Группа — scope для парковки (например API-аккаунт / ключ). При Publish задаётся `Task.Groups`; пустые → `_default`. Типичный кейс — воркеры ходят во внешний API с квотами: порядок на ресурс + лимит на аккаунт. Подробнее в [доке](https://rinsvent.github.io/smart-redis-queue/features/rate-limit/).

```go
producer.Publish(ctx, &redisqueue.Task{
    ID:        "msg-1",
    Partition: "!acct_9f3a:order_8841", // порядок по ресурсу
    Groups:    []string{"acct:9f3a"},   // одна группа на партицию — оптимально
    Payload:   []byte(`{}`),
    Scheduled: time.Now(),
})

consumer.Consume(ctx, func(task *redisqueue.Task) error {
    if accountRateLimited {
        // Паркует всю группу: Get не обходит её ready, пока не истечёт Delay.
        // Для ordered (!) дополнительно блокируется сама партиция в blocked.
        return redisqueue.NewRejectWithDelay(errors.New("rate limit"), 0.3, "acct:9f3a")
    }
    return nil
})

// Без задачи — Consumer или Admin:
consumer.BlockGroups(ctx, 0.3, "acct:9f3a")
admin.BlockGroups(ctx, "my-queue", 0.3, "acct:9f3a")
```

#### Транспортный rate limit на Get (opt-in)

Чтобы не делать Get → внешний Allow → Reject, лимиты вшиваются в Get (алгоритмы как в [rate-limiter](https://github.com/Rinsvent/rate-limiter)). Два слоя AND:

- **Group** — квота аккаунта/токена по **текущей** группе обхода Get; deny → блок этой группы на `RetryAfter`
- **Partition** — квота ресурса (заказ, документ, mailbox); deny → `blocked` только этой партиции

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
Разные внешние API — обычно разные очереди/consumer с разными Rates. Не путать с backpressure по тегу (F3) и с ручным `RejectWithDelay` в handler.

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

### Сжатие payload (Producer)

Большие JSON/текст в Redis жрут память и раздувают EVAL ARGV. Opt-in на продюсере (порог по умолчанию 1KiB):

```go
producer.SetCompression(redisqueue.CompressionZstd)
// или на вызов: PublishWith(ctx, []PublishOption{WithCompression(CompressionZstd)}, tasks...)
```

В Redis значение с маркером `RQC` + zstd; Get отдаёт уже разжатый `Task.Payload`. Старые raw-payload без маркера работают как раньше. Без `SetCompression` — ноль CPU на сжатие.

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
| `seed` | Демо-данные на нескольких DB (`demo-mail` / `demo-tg` / `demo-mixed`) |
| `publish` | Публиковать заглушечные задачи (поток для живого UI) |
| `consume` | Consume с заглушкой + stats (график g/a/r в UI) |
| `desktop` | GUI manager (Wails v3); локально лучше `./cmd/desktop` с `-tags wails,production,gtk3` |
| `server` | Web UI (тот же frontend); сборка `make server` → `./bin/manager-server server` |

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

# Демо для Desktop UI (DB 0,1,2)
./bin/manager seed --dbs 0,1,2

# Живой publish / consume (нагрузка «много партиций», правильный BlockGroups)
# Publish быстрее воркеров; глубина режется backpressure ~10k±jitter.
# Consume: 8 воркеров, sleep 20ms–2s (95% ≤100ms), stub-429 → BlockGroups.
./bin/manager --db 0 publish -q demo-mail
./bin/manager --db 0 consume -q demo-mail
# В UI: график → Queue rates / Redis host (INFO ops, CPU, SLOWLOG)
```

Глобальные флаги: `-a` (addr), `-P` (password), `--db`. Переменные: `REDIS_ADDR`, `REDIS_PASSWORD`.

### Desktop / Mobile UI (Wails v3)

GUI для Redis-хостов (без выбора DB в профиле): обзор очередей по всем DB, режимы Partitions / Groups / Tags, purge, block/unblock партиций (rate-limit ZSET `blocked`), retry, payload по ID и операции по тегам.

Стек: **Wails v3** (`github.com/wailsapp/wails/v3`). Сборка GUI за build-тегом `wails` (плюс `android` / `ios` для мобилок), чтобы обычный `go build ./...` и тесты библиотеки не требовали CGO/GTK.

Профиль: `name` + `addr` + `password`. Каталог очередей строится SCAN’ом `queue:*:partitions` и `queue:*:consumers` по DB `0..N-1` (N из `CONFIG GET databases`, иначе 16). Обновление каталога — раз в 5 минут и кнопкой Refresh. Одноимённые очереди на разных DB схлопываются в списке; в карточке — табы по DB.

#### Готовые бинарники (GitHub Releases)

На тег `v*` (или вручную **Actions → Release Desktop → Run workflow**) собирается:

| Asset | Что это |
| --- | --- |
| `smart-redis-queue-desktop-*` | **GUI**: запуск без аргументов открывает окно (двойной клик / `./…-desktop-…`) |
| `smart-redis-queue-cli-*` | **CLI**: `info` / `purge` / `retry` / `seed` / … |
| `smart-redis-queue-android.apk` | **Android** APK (experimental Wails v3) |
| `smart-redis-queue-ios-simulator-arm64.app.zip` | **iOS Simulator** `.app` (experimental; ad-hoc). Device/IPA — локально с Apple signing |

Платформы desktop: `linux-amd64`, `windows-amd64.exe`, `darwin-arm64`, `darwin-amd64`.

```bash
# UI
./smart-redis-queue-desktop-linux-amd64

# CLI
./smart-redis-queue-cli-linux-amd64 info
```

Linux / NVIDIA: `WEBKIT_DISABLE_DMABUF_RENDERER=1` выставляется приложением (и `WebviewGpuPolicy=Never`). При упрямстве Wayland: `GDK_BACKEND=x11`.

Windows: нужен [WebView2 Runtime](https://developer.microsoft.com/microsoft-edge/webview2/).  
macOS: без notarization — при первом запуске **Open** из контекстного меню (Gatekeeper).

#### Сборка из исходников

Требования:

- Go 1.25+ (см. `go.mod`), CGO, [Wails v3](https://v3.wails.io/getting-started/installation/)
- Node.js 18+ (frontend; Vite 6)
- Linux desktop: GTK3 + WebKit2GTK 4.1 и тег `gtk3` (legacy path Wails v3; GTK4/WebKitGTK 6.0 — дефолт v3, у нас в CI пока GTK3)
- Redis для проверки подключений из UI

```bash
# CLI wails3 (один раз; на Linux без gtk4 ставьте с CGO_ENABLED=0)
CGO_ENABLED=0 go install github.com/wailsapp/wails/v3/cmd/wails3@v3.0.0-beta.31

# Frontend
cd internal/desktopui/frontend && npm install && npm run build && cd -

# Desktop GUI
make desktop && ./bin/smart-redis-queue

# или
go run -tags wails,production,gtk3 ./cmd/desktop
```

Зависимости ОС (Ubuntu 24.04, legacy GTK3):

```bash
sudo apt install libgtk-3-dev libwebkit2gtk-4.1-dev
```

Android (experimental; нужны SDK/NDK 26.3.x, JDK):

```bash
wails3 doctor
wails3 task android:run          # emulator
wails3 task android:package:fat  # APK
```

В логах CI при bindings может быть `could not import C (no metadata for C)` — это нормально (генерация без CGO/NDK), на APK не влияет.  
На телефоне Redis по умолчанию (`localhost`) недоступен: в UI добавьте адрес хоста/VPN.

iOS (experimental; только macOS + полный Xcode):

```bash
wails3 doctor
wails3 task ios:run                              # Simulator
wails3 task ios:package                          # production .app (simulator)
# Device / IPA (нужны Apple Development identity + provisioning):
# wails3 task ios:package IOS_PLATFORM=device CODESIGN_IDENTITY="Apple Development: …"
# wails3 task ios:package:ipa IOS_PLATFORM=device …
```

CI кладёт в Release zip симуляторного `.app`. Для App Store / устройства — подпись локально или через `wails3 task ios:xcode`.

Профили: `~/.config/smart-redis-queue/connections.json` (или `os.UserConfigDir()`). При старте GUI есть **Local Redis** (`localhost:6379`).

Иконка: `internal/desktopui/appicon.png` / `build/appicon.png`. На Linux пишутся PNG + `.desktop` с `StartupWMClass` = basename бинаря.

CLI: `make manager && ./bin/manager info`. Подкоманда `manager desktop` — только с `-tags wails`; для GUI — `./cmd/desktop`.

#### Web UI (`server`) — тот же frontend без desktop

Wails v3 **server mode**: HTTP + WebSocket, без CGO/GTK. Коллеги открывают браузер.

```bash
make server
# только локально:
./bin/manager-server server --port 8080
# в LAN (коллеги → http://<ваш-ip>:8080 ):
./bin/manager-server server --host 0.0.0.0 --port 8080
```

Флаги: `--host` / `-H`, `--port` / `-p`, `--allow-any-origin` (если Origin ломает WS за прокси).  
Профили connections — как у desktop (`~/.config/smart-redis-queue/...` на сервере).

Важно: бинарь с `-tags server` и desktop (`-tags wails`) — разные сборки; в одном файле оба режима не совмещаются.

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
