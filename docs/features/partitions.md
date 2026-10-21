# Партиции, порядок, приоритет, delayed

## Партиция

`Task.Partition` — ключ маршрутизации внутри очереди.

- Без префикса `!` — несколько consumer’ов могут брать задачи параллельно.
- С префиксом `!` — **ordered**: партицию в каждый момент держит один consumer, порядок сохраняется.

```go
producer.Publish(ctx,
    &redisqueue.Task{ID: "1", Partition: "!order:8841", Payload: []byte("a"), Scheduled: time.Now()},
    &redisqueue.Task{ID: "2", Partition: "!order:8841", Payload: []byte("b"), Scheduled: time.Now()},
)
// Всегда: сначала 1, потом 2
```

Если воркер упал, heartbeat истекает (~120s) и задача возвращается в очередь с исходным score — порядок не ломается.

## Приоритет

Внутри партиции больший `Priority` идёт раньше:

```go
producer.Publish(ctx,
    &redisqueue.Task{ID: "low", Partition: "p1", Priority: 1, Payload: []byte("low"), Scheduled: time.Now()},
    &redisqueue.Task{ID: "high", Partition: "p1", Priority: 10, Payload: []byte("high"), Scheduled: time.Now()},
)
// high, затем low
```

## Отложенный запуск

```go
producer.Publish(ctx, &redisqueue.Task{
    ID:        "later",
    Payload:   []byte("data"),
    Scheduled: time.Now().Add(5 * time.Minute),
})
```

До наступления `Scheduled` задача в Get не попадёт.

## Пачка из одной партиции

Обычный `Get` с `prefetchCount > 1` собирает пачку из разных партиций. Если consumer склеивает задачи в один внешний запрос (например, профили пользователей одного аккаунта одним вызовом API), нужна пачка из одной партиции:

```go
consumer.SetPrefetchCount(100)
consumer.SetSamePartitionBatch(true)
tasks, _ := consumer.Get(ctx) // все задачи из одной партиции
```

- Первая задача выбирается как обычно: случайная свободная группа и партиция.
- Остальные до `prefetchCount` берутся только из её партиции — по приоритету и `Scheduled <= now`.
- Готовых задач меньше — пачка меньше, Get их не ждёт.
- Заблокированная группа и ordered-партиция работают как обычно: пачка ordered-партиции держит её лок до Ack / Reject.

Без опции поведение не меняется.

## Батч Publish

```go
err := producer.Publish(ctx, tasks...)
// либо все задачи записались, либо ошибка (в том числе из‑за дубликатов ID)
```

## Как называть партиции

Для внешних API с порядком на ресурс удобна схема:

```text
!{account_or_token}:{resource_id}
```

Примеры:

| Домен | Partition |
|-------|-----------|
| Платёжный API | `!merchant_12:payment_9001` |
| Почтовый провайдер | `!mailbox_ab:thread_55` |
| Geocoding / Maps | `!project_7:batch_3` |
| LLM API | `!org_42:conversation_9` |

Группу для парковки квоты аккаунта задавайте отдельно — см. [Rate limit](rate-limit.md).
