# Backpressure и теги

## Теги

Индексация задач для подсчёта и массового удаления. Задаются при Publish. Разделитель в Redis — `#` (`TagSeparator`); символ `#` внутри значения запрещён.

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

### Admin: удаление

```go
res, _ := admin.Remove(ctx, "my-queue", "msg-1")
// Status: "removed" | "inprogress" | "missing"

for {
    batch, err := admin.RemoveByTag(ctx, "my-queue", "campaign:spring", redisqueue.RemoveByTagOptions{
        Limit:         100, // default 100, max 1000
        ReturnPayload: true,
    })
    if err != nil || len(batch) == 0 {
        break
    }
}
```

`CountByTag` считает pending + in-progress; после remove/ack уже нет.

## Tag backpressure (Publish)

Opt-in лимит глубины: продюсер не заливает задачи, пока по тегу уже «полный» буфер (`SCARD` + размер батча > limit). При отказе **ни одна** задача из вызова не добавляется → `ErrBackpressure`.

```go
err := producer.PublishWith(ctx, []redisqueue.PublishOption{
    redisqueue.WithTagBackpressure("provider:ses", 100),
}, &redisqueue.Task{
    ID: "m1", Tags: []string{"provider:ses"}, Payload: []byte(`{}`), Scheduled: time.Now(),
})

var bp *redisqueue.ErrBackpressure
if errors.As(err, &bp) {
    // подождать / уменьшить батч; bp.Tag, bp.Count, bp.Limit
}
```

Несколько лимитов — AND (срабатывает первый превышенный):

```go
redisqueue.WithTagLimits(map[string]int64{"provider:ses": 100, "campaign:spring": 200})
```

Рекомендация: один стабильный тег потока на задачу. Не путать с rate limit по времени и с idempotency TTL.
