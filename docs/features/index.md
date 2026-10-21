# Фичи

Тяжёлые режимы включаются явно. Без флага горячий путь остаётся простым.

| Фича | Зачем | Страница |
|------|--------|----------|
| Ordered-партиции (`!`) | Строгий порядок, один consumer | [Партиции](partitions.md) |
| Priority / Scheduled | Важное раньше; отложенный запуск | [Партиции](partitions.md) |
| Groups + BlockGroups | Парковка scope (аккаунт, ключ API) | [Rate limit](rate-limit.md) |
| Пауза без сокращения | Короткий 429 не сокращает длинную паузу группы | [Rate limit](rate-limit.md) |
| Пачка из одной партиции | Склеить задачи одного ресурса в один внешний запрос | [Партиции](partitions.md) |
| Rate limit на Get | Два слоя лимитов до выдачи задачи | [Rate limit](rate-limit.md) |
| Idempotency TTL | Не принимать тот же ID после успешного Ack | [Debounce / идемпотентность](debounce-idempotency.md) |
| Debounce (`deb!`) | В очереди всегда последний payload | [Debounce / идемпотентность](debounce-idempotency.md) |
| Concurrency tags (`race!`) | Один shared-ресурс — один in-flight | [race!](race.md) |
| Tag backpressure | Не раздувать очередь по тегу | [Backpressure / теги](backpressure-tags.md) |
| Tags + Admin | Подсчёт и массовое удаление | [Backpressure / теги](backpressure-tags.md) |
| zstd | Меньше памяти Redis на крупных payload | [Сжатие / метрики](compression-stats.md) |
| Stats | get/ack/reject в Redis и в UI | [Сжатие / метрики](compression-stats.md) |
| Шардинг | Несколько Redis, sticky ByGroup/ByManual | [Шардинг](../sharding.md) |
| Step recovery | Checkpoint шагов handler (`NewSteps`) | [Step recovery](step-recovery.md) |
| Headers | Мета между попытками (ошибки и т.п.) | [Headers](headers.md) |

## Prefetch и пул

```go
consumer.SetPrefetchCount(10)
consumer.SetPollInterval(500 * time.Millisecond)

pool := redisqueue.NewConsumerPool(rdb, "my-queue")
pool.SetCount(5)
pool.Consume(ctx, process)
```

Для ordered-партиций: если первая задача из prefetch-батча ушла в Reject, остальные тоже возвращаются с сохранением порядка.

Паника в handler процесс не роняет — превращается в ошибку и идёт в Reject.

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

Сигнатуры — в [API](../reference/api.md).
