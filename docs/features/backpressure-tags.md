# Backpressure и теги

## Теги

Теги задаются при Publish. По ним можно считать задачи и удалять пачками. В значении тега нельзя использовать `#` — это разделитель внутри Redis.

```go
producer.Publish(ctx, &redisqueue.Task{
    ID:        "msg-1",
    Payload:   []byte(`{"to":"user@example.com"}`),
    Tags:      []string{"campaign:spring", "provider:ses"},
    Scheduled: time.Now(),
})

admin := redisqueue.NewAdmin(rdb)
n, _ := admin.CountByTag(ctx, "my-queue", "campaign:spring")
```

`CountByTag` считает pending и in-progress. После remove или Ack задача из индекса пропадает.

### Удаление через Admin

```go
res, _ := admin.Remove(ctx, "my-queue", "msg-1")
// Status: "removed" | "inprogress" | "missing"

for {
    batch, err := admin.RemoveByTag(ctx, "my-queue", "campaign:spring", redisqueue.RemoveByTagOptions{
        Limit:         100, // по умолчанию 100, максимум 1000
        ReturnPayload: true,
    })
    if err != nil || len(batch) == 0 {
        break
    }
}
```

## Backpressure на Publish

Можно ограничить глубину очереди по тегу. Перед записью смотрится `SCARD` + размер батча; если лимит превышен, **ни одна** задача из вызова не пишется — ошибка `ErrBackpressure`.

```go
err := producer.PublishWith(ctx, []redisqueue.PublishOption{
    redisqueue.WithTagBackpressure("provider:ses", 100),
}, &redisqueue.Task{
    ID: "m1", Tags: []string{"provider:ses"}, Payload: []byte(`{}`), Scheduled: time.Now(),
})

var bp *redisqueue.ErrBackpressure
if errors.As(err, &bp) {
    // подождать или уменьшить батч; bp.Tag, bp.Count, bp.Limit
}
```

Несколько лимитов — все должны пройти:

```go
redisqueue.WithTagLimits(map[string]int64{"provider:ses": 100, "campaign:spring": 200})
```

Удобнее один стабильный тег потока на задачу. Это не rate limit по времени и не окно по ID после Ack.
