# Группы и rate limit

Типичный сценарий: воркеры ходят во внешний API и обязаны соблюдать его квоты — без шторма «взяли задачу → поняли, что нельзя → вернули».

## Из чего складывается

| Сущность | Роль |
|----------|------|
| **Partition** | Порядок и локальный лимит на ресурс (`!acct:resource`) |
| **Group** | Парковка всего аккаунта / ключа (`acct:…`) |
| **Rate limit на Get** | Лимиты до выдачи задачи (группа и партиция) |
| **RejectWithDelay / BlockGroups** | Парковка после 429 или MAX от провайдера |

Если `Task.Groups` пустой — используется `_default`.

## Publish

```go
producer.Publish(ctx, &redisqueue.Task{
    ID:        "sync-1",
    Partition: "!acct_9f3a:doc_1204",
    Groups:    []string{"acct:9f3a"},
    Payload:   []byte(`{"op":"export"}`),
    Scheduled: time.Now(),
})
```

Одна группа на партицию — самый простой и предсказуемый вариант.

## Rate limit на Get

Лимиты можно повесить на Get. Алгоритмы те же, что в [rate-limiter](https://github.com/Rinsvent/rate-limiter):

`token-bucket`, `leaky-bucket`, `fixed-window-counter`, `sliding-window-log`, `sliding-window-counter`.

Два слоя работают вместе:

- **Group** — квота аккаунта / ключа; при отказе группа блокируется на `RetryAfter`
- **Partition** — квота ресурса; при отказе блокируется только эта партиция

```go
_ = consumer.SetRateLimit(&redisqueue.RateLimitConfig{
    Algorithm: redisqueue.RateLimitTokenBucket,
    Group: &redisqueue.RateLimitLayer{
        Rates: []redisqueue.Rate{{Duration: time.Second, Value: 30}},
    },
    Partition: &redisqueue.RateLimitLayer{
        Rates: []redisqueue.Rate{
            {Duration: time.Second, Value: 1},
            {Duration: time.Minute, Value: 20},
        },
    },
})
```

Любой слой можно не задавать. Без `SetRateLimit` Get работает как раньше, без ключей `rl:*`.

Разные провайдеры (почта, карты, биллинг) обычно разносят по разным очередям со своими Rates.

## Парковка после 429

```go
consumer.Consume(ctx, func(task *redisqueue.Task) error {
    err := callExternalAPI(task)
    if isRateLimited(err) {
        // Паркует всю группу: Get её не обходит, пока не истечёт Delay.
        // Для ordered (!) дополнительно блокируется сама партиция.
        return redisqueue.NewRejectWithDelay(err, 2.0, "acct:9f3a")
    }
    return err
})
```

Без задачи под рукой:

```go
consumer.BlockGroups(ctx, 2.0, "acct:9f3a")
admin.BlockGroups(ctx, "jobs", 2.0, "acct:9f3a")
```

### Пауза без сокращения

По умолчанию каждая парковка перезаписывает время разблокировки группы. Если несколько consumer'ов одной группы одновременно получили 429 с разным `Retry-After`, последний записавший побеждает: пауза на 60 с может превратиться в паузу на 1 с, и аккаунт снова пойдёт в API раньше, чем провайдер разрешил.

Опция включает `ZADD GT` (Redis ≥ 6.2): пауза только продлевается, более короткая её не сокращает.

```go
pool.SetExtendGroupBlocksOnly(true) // или consumer.SetExtendGroupBlocksOnly(true)
```

Действует на `Reject` с `BlockGroups` и на `Consumer.BlockGroups`. `Admin.BlockGroups` всегда перезаписывает — это ручное управление, им можно и сократить паузу.

### Зачем парковать группу

Если при лимите аккаунта reject’ить только партиции ресурсов, Get продолжает обходить тысячи партиций — Redis CPU растёт вместе с их числом.

С `BlockGroups` простой Get по запаркованной группе почти не зависит от числа партиций. Цифры — в [Бенчмарках](../benchmarks.md).

## Как это лежит в Redis

- `queue:{name}:groups` — ZSET; score `≤ now` значит группа свободна
- `queue:{name}:blocked` — заблокированные партиции (время разблокировки)
- готовые группы обходятся порциями со случайного места, без загрузки миллионов партиций в один Lua-вызов
- свободных групп нет — Get пустой

## Примеры доменов

| API | Group | Partition |
|-----|-------|-----------|
| Платежи | `merchant:{id}` | `!merchant:{id}:payment:{pid}` |
| Email (SES / SendGrid) | `domain:{name}` | `!domain:{name}:recipient:{hash}` |
| Google Maps | `project:{id}` | `!project:{id}:job:{jid}` |
| OpenAI / LLM | `org:{id}` | `!org:{id}:thread:{tid}` |
| GitHub REST | `token:{hash}` | `!token:{hash}:repo:{nwo}` |

Не путать с [backpressure по тегу](backpressure-tags.md) (глубина при Publish) и с [idempotency TTL](debounce-idempotency.md) (повтор того же ID после Ack).
