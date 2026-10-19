# Concurrency tags (`race!`)

Mutex на ресурс **между** партициями. Пока задача in-flight держит лок, другая партиция с тем же `race!` тегом не выдаётся.

## Зачем

Порядок внутри партиции (`!`) не мешает двум разным ключам параллельно трогать один и тот же внешний объект.

Например:

- один файл или хеш грузится из разных job’ов;
- один wallet не должен обрабатываться двумя воркерами сразу;
- один document id приходит из нескольких пайплайнов.

## Как пользоваться

На Publish кладёте тег с префиксом `race!`. Проверка и локи включаются только после `SetConcurrencyTags(true)`:

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

Ack и Reject снимают лок. Без `SetConcurrencyTags` Get ничего лишнего не делает (индекс на Publish всё равно пишется).

## Вместе с ordered

| Механизм | Граница |
|----------|---------|
| `!partition` | Порядок и эксклюзив внутри партиции |
| `race!tag` | Эксклюзив между партициями по общему ресурсу |

Часто комбинируют: ordered по бизнес-ключу и `race!` на тяжёлый shared artifact.
