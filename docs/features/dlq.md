# Dead letter queue (DLQ)

Терминальный сбой: задача уходит из pending в единый ZSET `queue:{name}:dlq`, без приоритетов и партиционных ключей.

## Reject → DLQ

```go
return redisqueue.NewRejectWithDelay(err, redisqueue.WithDeadLetter())
// или
_ = consumer.Reject(ctx, task.ID, redisqueue.WithDeadLetter())
```

- Meta (`payload`, `partition`, `priority`, `headers`, `step`, tags, `reject_count`) **не** чистится.
- `srq:queues` −1 (как при Ack).
- Race / ordered locks снимаются как при обычном Reject.
- `WithDelay` / `WithBlockGroups` / `WithBlockTags` по-прежнему паркуют scope, если переданы вместе с DLQ.

## Admin

| Метод | Действие |
|-------|----------|
| `CountDLQ` / `ListDLQ` | Сколько / страница (score = время попадания) |
| `RetryDLQ(queue, ids...)` | Назад в pending с исходным score; пустой ids = все; `srq:queues` +N |
| `PurgeDLQ(queue, ids...)` | Удалить из DLQ + meta (как Ack); depth не трогаем |

`InspectTask` / `ListMessages` отдают state `dead_letter`, пока задача в DLQ.

Ключи: [redis-keys](../reference/redis-keys.md) (`dlq`, `dlq:score:{id}`).
