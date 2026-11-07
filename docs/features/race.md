# Concurrency tags (`race!` / `race!s!`)

Mutex на ресурс **между** партициями. Пока задача in-flight держит лок, другая партиция с тем же тегом не выдаётся.

Для prepare-once (загрузить файл один раз, дальше рассылка параллельно) — префикс `race!s!`: после `Share` / `Ack(WithShared(...))` лок больше не нужен, а blob приходит в `Task.Shared` на Get.

## Зачем

Порядок внутри партиции (`!`) не мешает двум разным ключам параллельно трогать один и тот же внешний объект.

Примеры:

- один wallet / document id — классический `race!` (mutex на весь processing);
- один набор файлов для рассылки в N чатов — `race!s!` (один prepare, потом parallel).

## Как пользоваться

На Publish кладёте тег с префиксом `race!` или `race!s!`. Проверка и локи — только после `SetConcurrencyTags(true)`:

```go
consumer.SetConcurrencyTags(true)

_ = producer.Publish(ctx, &redisqueue.Task{
    ID:        "job-1",
    Partition: "!chat:1",
    Tags:      []string{redisqueue.RaceSharedTag("file:" + fileHash)},
    Payload:   payload,
    Scheduled: time.Now(),
})
```

### Классический mutex (`race!`)

Ack и Reject снимают лок. Без `SetConcurrencyTags` Get ничего лишнего не делает (индекс на Publish всё равно пишется).

### Prepare-once (`race!s!{id}`)

| Момент | Поведение |
|--------|-----------|
| Shared ещё нет | как `race!`: один in-flight, остальные ждут |
| `Share` или `Ack(WithShared(...))` | `SET queue:{q}:shared:{id}` + TTL, лок снят |
| Shared уже есть | Get **без** лока, `task.Shared[id]` = blob; полный параллелизм |
| Поздний Publish с тем же тегом | сразу parallel + Shared |

Ключ Shared — **отдельная** строка `queue:{q}:shared:{id}` с `EXPIRE` (Redis 6+, без HEXPIRE на hash-field).

```go
// Get / Consume: Shared уже на задаче
if len(task.Shared["file:"+hash]) == 0 {
    tokens := uploadToTelegram(...) // лидер
    // быстрее разблокировать тег до конца хэндлера:
    _ = consumer.Share(ctx, task.ID, map[string][]byte{"file:" + hash: tokens},
        redisqueue.WithSharedTTL(24*time.Hour))
    // или отложить на Ack / Consume:
    // task.SetAckShared(map[string][]byte{"file:" + hash: tokens})
}
sendWithTokens(task.Payload, task.Shared["file:"+hash])
```

`Share` и `WithShared` **перезаписывают** blob (удобно обновить токены).

`Ack` — variadic options, только нужные:

```go
_ = consumer.Ack(ctx, task.ID,
    redisqueue.WithIdempotency(time.Minute),
    redisqueue.WithShared(map[string][]byte{"file:" + hash: tokens}),
)
```

В `Consume` авто-Ack подхватит `task.SetAckShared(...)`; mid-handler `Share` разблокирует раньше.

## Без фичи очереди

Можно вынести prepare в отдельную задачу / app Redis и публиковать send без race-тегов. Обвязки больше — для простого кейса удобнее `race!s!`.

## Вместе с ordered

| Механизм | Граница |
|----------|---------|
| `!partition` | Порядок и эксклюзив внутри партиции |
| `race!tag` | Mutex между партициями на весь in-flight |
| `race!s!id` | Mutex только до Shared; дальше parallel + blob на Get |

Во время prepare Get может skip’ать занятые партиции (бюджет `maxRaceSkips`) — это ожидаемо, пока лидер не сделал `Share`/`WithShared`.
