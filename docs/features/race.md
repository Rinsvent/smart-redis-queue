# Concurrency tags (`race!`)

Mutex ресурса **между партициями**. Пока задача in-flight держит лок, другая партиция с тем же `race!` тегом не выдаётся.

## Зачем

Порядок внутри партиции (`!`) не защищает от параллельной работы над одним внешним ресурсом из разных ключей.

Примеры:

- один и тот же файл/хеш грузится из разных job’ов;
- один wallet/ledger не должен обрабатываться двумя воркерами сразу;
- один upstream document id приходит из нескольких пайплайнов.

## Использование

На Publish кладётся тег с префиксом `race!`. Проверка и HASH-локи — только при `SetConcurrencyTags(true)`:

```go
consumer.SetConcurrencyTags(true)

_ = producer.Publish(ctx, &redisqueue.Task{
    ID:        "job-1",
    Partition: "!acct_a:batch_1",
    Tags:      []string{redisqueue.RaceTagPrefix + "file:" + fileHash},
    Payload:   payload,
    Scheduled: time.Now(),
})
```

Ack / Reject снимают лок (если опция включена). Без `SetConcurrencyTags` — нулевой оверхед Get (индекс на Publish всё равно пишется).

## Связь с ordered

| Механизм | Граница |
|----------|---------|
| `!partition` | Порядок и эксклюзив **внутри** партиции |
| `race!tag` | Эксклюзив **между** партициями по общему ресурсу |

Часто комбинируют: ordered по бизнес-ключу + `race!` на тяжёлый shared artifact.
