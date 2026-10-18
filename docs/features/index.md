# Фичи

Все тяжёлые возможности — **opt-in**: без флага hot path остаётся лёгким.

| Фича | Зачем | Страница |
|------|--------|----------|
| Ordered-партиции (`!`) | Строгий порядок + один consumer | [Партиции](partitions.md) |
| Priority / Scheduled | Важное раньше; delayed jobs | [Партиции](partitions.md) |
| Groups + BlockGroups | Парковка scope (аккаунт / ключ API) | [Rate limit](rate-limit.md) |
| Get-side RateLimit | Dual RL до выдачи задачи | [Rate limit](rate-limit.md) |
| Idempotency TTL | Окно dedupe после Ack | [Debounce / idempotency](debounce-idempotency.md) |
| Debounce (`deb!`) | Всегда последний payload | [Debounce / idempotency](debounce-idempotency.md) |
| Concurrency tags (`race!`) | Mutex ресурса между партициями | [race!](race.md) |
| Tag backpressure | Лимит глубины на Publish | [Backpressure / tags](backpressure-tags.md) |
| Tags + Admin | Подсчёт / массовое удаление | [Backpressure / tags](backpressure-tags.md) |
| zstd | Меньше RAM Redis на крупных payload | [Compression / stats](compression-stats.md) |
| Stats | get/ack/reject в Redis + UI | [Compression / stats](compression-stats.md) |
| Prefetch / Pool / Middleware | Throughput и наблюдаемость | ниже и [API](../reference/api.md) |

## Prefetch и пул

```go
consumer.SetPrefetchCount(10)
consumer.SetPollInterval(500 * time.Millisecond)

pool := redisqueue.NewConsumerPool(rdb, "my-queue")
pool.SetCount(5)
pool.Consume(ctx, process)
```

Для ordered-партиций reject первой задачи в prefetch-батче отклоняет остальные с сохранением порядка.

Паника в handler не роняет процесс: `HandlerPanicError` → Reject.

## Middleware

```go
pool.Use(func(next redisqueue.HandlerFunc) redisqueue.HandlerFunc {
    return func(task *redisqueue.Task) error {
        err := next(task)
        if err != nil {
            log.Printf("task %s: %v", task.ID, err)
        }
        return err
    }
})
```
