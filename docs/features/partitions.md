# Партиции, порядок, приоритет, delayed

## Партиция

`Task.Partition` — ключ маршрутизации внутри очереди.

- Без префикса `!` — unordered: несколько consumer’ов могут брать задачи параллельно.
- С префиксом `!` — **ordered**: в каждый момент партицию держит один consumer; порядок строго сохраняется.

```go
producer.Publish(ctx,
    &redisqueue.Task{ID: "1", Partition: "!order:8841", Payload: []byte("a"), Scheduled: time.Now()},
    &redisqueue.Task{ID: "2", Partition: "!order:8841", Payload: []byte("b"), Scheduled: time.Now()},
)
// Всегда: 1, затем 2
```

### Reclaim

При падении воркера heartbeat истекает (~120s), задача возвращается в ZSET с **исходным score** (не `priority+1`). Порядок не ломается.

## Приоритет

Внутри партиции больший `Priority` обрабатывается раньше:

```go
producer.Publish(ctx,
    &redisqueue.Task{ID: "low", Partition: "p1", Priority: 1, Payload: []byte("low"), Scheduled: time.Now()},
    &redisqueue.Task{ID: "high", Partition: "p1", Priority: 10, Payload: []byte("high"), Scheduled: time.Now()},
)
// high, затем low
```

## Delayed / Scheduled

```go
producer.Publish(ctx, &redisqueue.Task{
    ID:        "later",
    Payload:   []byte("data"),
    Scheduled: time.Now().Add(5 * time.Minute),
})
```

Задача недоступна Get, пока `Scheduled` не наступит.

## Батч Publish

```go
err := producer.Publish(ctx, tasks...)
// атомарно: либо все, либо ошибка (в т.ч. дубликаты ID)
```

## Рекомендации по имени партиции

Для внешних API с порядком на ресурс:

```text
!{account_or_token}:{resource_id}
```

Примеры:

| Домен | Partition |
|-------|-----------|
| Платёжный API | `!merchant_12:payment_9001` |
| Почтовый провайдер | `!mailbox_ab:thread_55` |
| Maps / geocode | `!project_7:batch_3` |
| LLM API | `!org_42:conversation_9` |

Группу для парковки квоты аккаунта задавайте отдельно — см. [Rate limit](rate-limit.md).
