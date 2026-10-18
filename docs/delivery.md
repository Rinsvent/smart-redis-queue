# Гарантии доставки

## At-least-once

Базовая модель:

1. **Get** — задача выдаётся consumer’у, держится heartbeat’ом.
2. Успех → **Ack** (ключ убирается или остаётся на idempotency TTL).
3. Ошибка → **Reject** (опционально с delay / block group).
4. Падение воркера → **reclaim** по истечении TTL lock (~120s), задача снова в очереди.

Сообщение может быть обработано **более одного раза**, если Ack не успел, а reclaim уже вернул задачу. Handler должен быть готов к ретраям.

## Чего брокер не обещает

| Обещание | SRQ |
|----------|-----|
| At-most-once без потерь | Нет (по умолчанию at-least-once) |
| Exactly-once без идемпотентности handler | Нет |
| Kafka EOS (транзакции брокера) | Нет |
| Fan-out каждому подписчику | Нет |

## Как собрать «ровно один раз» по бизнесу

Эффективный exactly-once *результата*:

1. Стабильный `Task.ID` (бизнес-ключ).
2. Идемпотентный side-effect (upsert по ключу, unique constraint).
3. Опционально `SetIdempotencyTtl` — повторный Publish того же ID после успеха не примется, пока жив TTL.
4. Ordered `!` — убрать гонки порядка в партиции.

```go
consumer.SetIdempotencyTtl(24 * time.Hour)

consumer.Consume(ctx, func(task *redisqueue.Task) error {
    // upsert по task.ID / бизнес-ключу — безопасно при повторной доставке
    return applyOnce(task)
})
```

## Порядок

- Ordered-партиция (`!`): один consumer, reclaim сохраняет исходный ZSET score.
- Prefetch: reject первой → остальные из батча тоже с сохранением порядка.
- Unordered: параллелизм важнее порядка.

## Персист Redis

Гарантии SRQ живут поверх durability Redis:

- AOF `appendfsync always` / RDB + реплика — зона ответственности ops Redis;
- SRQ гарантирует протокол Get/Ack/Reject/reclaim, не «магию поверх потерянного Redis».

Чеклист persistence — в [Roadmap](roadmap.md).

## Связанные механизмы

- [Rate limit / BlockGroups](features/rate-limit.md) — не терять задачи при 429, а парковать scope
- [Debounce](features/debounce-idempotency.md) — latest-wins вместо шторма ретраев устаревших payload
- [race!](features/race.md) — не два in-flight на один shared resource
