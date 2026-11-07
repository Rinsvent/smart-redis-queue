# Concurrency tags (`race!` / `race!s!` / `race!b!`)

Mutex и prepare/wait-gate **между** партициями. Проверка — только после `SetConcurrencyTags(true)` (индекс тегов на Publish пишется всегда).

| Префикс | Ключ Redis | Поведение Get |
|---------|------------|---------------|
| `race!{id}` | `race:locks` | Mutex на весь in-flight |
| `race!s!{id}` | `shared:{id}` | Нет ключа → mutex; есть → parallel + blob в `Task.Shared` |
| `race!b!{id}` | `block:{id}` | Нет ключа → mutex (один probe); value ≠ `ready` → **skip**; `ready` → parallel без лока |

## Зачем

- `race!` — один wallet / document id, эксклюзив на время обработки.
- `race!s!` — один тяжёлый артефакт на пачку задач (prepare-once → parallel).
- `race!b!` — внешний ресурс «ещё не готов»: парковать Get на TTL (Reject), один probe после expiry, после успеха — `ready` и parallel.

## Классический mutex (`race!`)

```go
Tags: []string{redisqueue.RaceTagPrefix + "doc:" + docID}
```

Ack / Reject снимают лок.

## Prepare-once (`race!s!`)

```go
Tags: []string{redisqueue.RaceSharedTag("file:" + hash)}

if len(task.Shared["file:"+hash]) == 0 {
    artifact := prepareOnce(...)
    sh, err := redisqueue.NewShared(ctx, task)
    if err != nil { return err }
    if err := sh.Publish(ctx, map[string][]byte{"file:" + hash: artifact},
        redisqueue.WithSharedTTL(24*time.Hour)); err != nil {
        return err
    }
}
useArtifact(task.Payload, task.Shared["file:"+hash])
```

Либо `task.SetAckShared(...)` / `Ack(..., WithShared(...))`.

## Wait-gate (`race!b!`)

Пока внешняя обработка не готова — не выдавать задачи с тегом; после успеха — открыть parallel без лока в handler’е.

```go
Tags: []string{
    redisqueue.RaceSharedTag("file:" + hash),
    redisqueue.RaceBlockTag("batch:" + batchID),
}

// not ready → парк Get на Delay (block:{id}=wait), потом Reject
return redisqueue.NewRejectWithDelay(err, 30).WithBlockTags("batch:" + batchID)

// успех → артефакт + open gate
sh, _ := redisqueue.NewShared(ctx, task)
_ = sh.Publish(ctx, map[string][]byte{"file:" + hash: artifact},
    redisqueue.WithOpenBlocks("batch:"+batchID),
    redisqueue.WithSharedTTL(24*time.Hour))
```

Эквивалент open на Ack: `WithOpenBlocks(...)`.

Типичный поток batch + внешний processor:

1. Нет `shared` / нет `block` → один лидер (mutex), готовит артефакт, `Publish` shared.
2. Send → not ready → `RejectWithDelay.WithBlockTags` → все с `race!b!` skip, пока жив TTL.
3. TTL истёк → снова один probe (mutex на `race!b!`).
4. Успех → `Open` / `WithOpenBlocks` (`block:{id}=ready`) → остальные parallel; `shared` отдаёт blob.

Окно «сдаться через N минут» / backoff 1s→5s→30s — в handler (например по `RejectCount`).

## Без фичи очереди

Prepare/wait можно держать в app Redis + отдельные задачи. Обвязки больше — для простого кейса удобнее теги выше.

## Вместе с ordered

| Механизм | Граница |
|----------|---------|
| `!partition` | Порядок и эксклюзив внутри партиции |
| `race!` | Mutex in-flight |
| `race!s!` | Mutex до Shared, потом parallel + blob |
| `race!b!` | Skip пока wait; mutex probe; ready → parallel |

Во время wait/prepare Get может skip’ать занятые партиции (бюджет `maxRaceSkips`) — ожидаемо.
