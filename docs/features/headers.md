# Headers

Метаданные задачи, которые могут меняться между попытками (например `last_error` при Reject). Отдельный Redis HASH — **не** читается в Get.

## API

Задача должна быть получена через `Get` / `Consume` (есть bind redis+queue):

```go
pool.Consume(ctx, func(task *redisqueue.Task) error {
    if err := doWork(task); err != nil {
        _ = task.SetHeader(ctx, "last_error", err.Error())
        return err // Reject; headers остаются
    }
    return nil // Ack удаляет headers вместе с задачей
})
```

| Метод | Назначение |
|-------|------------|
| `Headers(ctx)` | HGETALL → `map[string]string` |
| `SetHeader(ctx, key, value)` | одно поле |
| `SetHeaders(ctx, map)` | полная замена; пустая map — удалить ключ |
| `DeleteHeader(ctx, key)` | одно поле |

## Жизненный цикл

| Событие | Headers |
|---------|---------|
| Reject / reclaim | остаются |
| Ack | удаляются |
| Admin.Remove | удаляются |

## Desktop / UI

Диалог **Task by ID** показывает payload и headers (если есть).

## Ключ Redis

`queue:{name}:headers:{taskID}` — HASH. См. [Ключи Redis](../reference/redis-keys.md).
