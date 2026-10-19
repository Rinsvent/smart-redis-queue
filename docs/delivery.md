# Гарантии доставки

## At-least-once

Базовый цикл:

1. **Get** — задача выдана consumer’у, держится heartbeat’ом.
2. Успех → **Ack**.
3. Ошибка → **Reject** (можно с delay или парковкой группы).
4. Воркер умер → по истечении TTL lock (~120s) задача возвращается в очередь (**reclaim**).

Сообщение может обработаться больше одного раза, если Ack не успел, а reclaim уже вернул задачу. Handler должен переживать ретраи.

## Чего брокер не обещает

| Обещание | SRQ |
|----------|-----|
| At-most-once без потерь | Нет (по умолчанию at-least-once) |
| Exactly-once без идемпотентности handler | Нет |
| Транзакции publish+consume как у Kafka EOS | Нет |
| Fan-out каждому подписчику | Нет |

## Как получить «ровно один раз» по бизнесу

1. Стабильный `Task.ID` (бизнес-ключ).
2. Идемпотентный side-effect (upsert, unique constraint).
3. По желанию — `SetIdempotencyTtl`: после успеха повторный Publish того же ID не примется, пока жив TTL.
4. Ordered `!`, если важен порядок в партиции.

```go
consumer.SetIdempotencyTtl(24 * time.Hour)

consumer.Consume(ctx, func(task *redisqueue.Task) error {
    // upsert по task.ID — безопасно при повторной доставке
    return applyOnce(task)
})
```

## Порядок

- Ordered (`!`): один consumer на партицию; reclaim сохраняет исходный score.
- Prefetch: reject первой задачи возвращает и остальные из батча с тем же порядком.
- Unordered: параллелизм важнее порядка.

## Персист Redis

Гарантии SRQ живут поверх durability Redis:

- AOF / RDB / реплика — зона ответственности ops;
- SRQ гарантирует протокол Get / Ack / Reject / reclaim, а не «данные переживут любой kill Redis».

Чеклист persistence — в [Roadmap](roadmap.md).

## Рядом по смыслу

- [Rate limit / BlockGroups](features/rate-limit.md) — не терять задачи при 429, а парковать scope
- [Debounce](features/debounce-idempotency.md) — свежий payload вместо пачки устаревших
- [race!](features/race.md) — не два in-flight на один shared resource
