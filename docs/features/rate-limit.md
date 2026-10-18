# Группы и rate limit

Главный сценарий SRQ: воркеры вызывают **внешний API с квотами** и должны им следовать, не устраивая шторм Get → Reject.

## Модель

| Сущность | Роль |
|----------|------|
| **Partition** | Порядок и локальный лимит на ресурс (`!acct:resource`) |
| **Group** | Scope парковки на аккаунт / API-ключ (`acct:…`) |
| **Get RateLimit** | Opt-in: deny **до** выдачи задачи (group ∧ partition) |
| **RejectWithDelay / BlockGroups** | Ручная парковка после 429 / MAX от провайдера |

Пустые `Task.Groups` → `_default`.

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

Одна группа на партицию — оптимально для обхода ready.

## Get-side RateLimit (opt-in)

Чтобы не делать Get → внешний Allow → Reject, лимиты вшиваются в Get. Алгоритмы как в [rate-limiter](https://github.com/Rinsvent/rate-limiter):

`token-bucket`, `leaky-bucket`, `fixed-window-counter`, `sliding-window-log`, `sliding-window-counter`.

Два слоя **AND**:

- **Group** — квота аккаунта / ключа; deny → блок группы на `RetryAfter`
- **Partition** — квота ресурса; deny → `blocked` только этой партиции

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

Любой слой можно опустить (`nil`). Без `SetRateLimit` — прежний Get без ключей `rl:*`.

Разные провайдеры (email / maps / billing) — обычно **разные очереди** и разные Rates.

## Парковка после 429

```go
consumer.Consume(ctx, func(task *redisqueue.Task) error {
    err := callExternalAPI(task)
    if isRateLimited(err) {
        // Паркует всю группу: Get не обходит её ready, пока не истечёт Delay.
        // Для ordered (!) дополнительно блокируется партиция в blocked.
        return redisqueue.NewRejectWithDelay(err, 2.0, "acct:9f3a")
    }
    return err
})
```

Без задачи:

```go
consumer.BlockGroups(ctx, 2.0, "acct:9f3a")
admin.BlockGroups(ctx, "jobs", 2.0, "acct:9f3a")
```

### Почему важна парковка группы

Если при лимите аккаунта reject’ить только партиции ресурса, Get продолжает обходить тысячи партиций → рост Redis CPU.

С `BlockGroups` idle Get под запаркованной группой **не растёт с N** (~80–95 µs Redis CPU на бенчах). См. [Бенчмарки](../benchmarks.md).

## Как устроено в Redis

- `queue:{name}:groups` (ZSET) — score `≤ now` свободна, `> now` блок
- `queue:{name}:blocked` — member=partition → unlockAt (ordered Reject / partition RL)
- Ready группы обходится через `ZRANGE` с random index (не грузит миллионы партиций в Lua разом)
- Нет свободных групп → Get пуст

## Примеры доменов

| API | Group | Partition |
|-----|-------|-----------|
| Stripe-like payments | `merchant:{id}` | `!merchant:{id}:payment:{pid}` |
| SendGrid / SES | `domain:{name}` | `!domain:{name}:recipient:{hash}` |
| Google Maps | `project:{id}` | `!project:{id}:job:{jid}` |
| OpenAI / LLM | `org:{id}` | `!org:{id}:thread:{tid}` |
| GitHub REST | `token:{hash}` | `!token:{hash}:repo:{nwo}` |

Не путать с [backpressure по тегу](backpressure-tags.md) (глубина Publish) и с [idempotency TTL](debounce-idempotency.md) (окно по ID после Ack).
