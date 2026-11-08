# Dead letter queue (DLQ)

Терминальный сбой: задача уходит из pending в единый ZSET `queue:{name}:dlq`, без приоритетов и партиционных ключей.

## Reject → DLQ

```go
return redisqueue.NewRejectWithDelay(err, redisqueue.WithDeadLetter())
// или с лимитом ёмкости:
return redisqueue.NewRejectWithDelay(err, redisqueue.WithDeadLetter(101))
// или
_ = consumer.Reject(ctx, task.ID, redisqueue.WithDeadLetter(101))
consumer.SetDLQMaxSize(101) // дефолт для WithDeadLetter() без аргумента
```

- Meta (`payload`, `partition`, `priority`, `headers`, `step`, tags, `reject_count`) **не** чистится (пока задача в DLQ).
- `srq:queues` −1 (как при Ack).
- Race / ordered locks снимаются как при обычном Reject.
- `WithDelay` / `WithBlockGroups` / `WithBlockTags` по-прежнему паркуют scope, если переданы вместе с DLQ.

### Ёмкость (max size)

| Вызов | Поведение |
|-------|-----------|
| `WithDeadLetter()` | Лимит из `SetDLQMaxSize` (0 = без лимита) |
| `WithDeadLetter(0)` | Явно без лимита |
| `WithDeadLetter(N)` N>0 | После добавления `ZCARD(dlq) ≤ N`: вытесняются самые старые (score = время попадания), meta чистится как при `PurgeDLQ` |

На Reject трим до 32 oldest за вызов (обычный путь: +1/−1). Если хвост больше — ping под `unlock:lock` дочищает батчами по 64, лимит из `SetDLQMaxSize` (ARGV, без ключа в Redis).

## Admin

| Метод | Действие |
|-------|----------|
| `CountDLQ` / `ListDLQ` | Сколько / страница (score = время попадания) |
| `RetryDLQ(queue, ids...)` | Назад в pending с исходным score; пустой ids = все; `srq:queues` +N |
| `PurgeDLQ(queue, ids...)` | Удалить из DLQ + meta (как Ack); depth не трогаем |

`InspectTask` / `ListMessages` отдают state `dead_letter`, пока задача в DLQ.

Ключи: [redis-keys](../reference/redis-keys.md) (`dlq`, `dlq:score:{id}`).
