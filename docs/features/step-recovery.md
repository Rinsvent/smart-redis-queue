# Step recovery

Helper для checkpoint внутри handler: несколько неатомарных шагов переживают падение воркера без повторного выполнения уже завершённых.

После `Get` / `Consume` у `Task` уже есть bind на redis и имя очереди (в т.ч. нужный шард) — `NewSteps` берёт их оттуда.

## Пример

```go
pool.Consume(ctx, func(task *redisqueue.Task) error {
    s, err := redisqueue.NewSteps(ctx, task)
    if err != nil {
        return err
    }

    if err := s.Do(ctx, "create_order", func(ctx context.Context) error {
        return createOrder(ctx, task)
    }); err != nil {
        return err
    }

    if err := s.Do(ctx, "call_api", callAPI); err != nil {
        return redisqueue.NewRejectWithDelay(err, 2.0)
    }

    return s.Do(ctx, "notify", sendNotify)
    // успех → Ack; ключ step удалится в Lua
})
```

Опционально TTL:

```go
s, err := redisqueue.NewSteps(ctx, task, redisqueue.StepsOptions{TTL: 7 * 24 * time.Hour})
```

Без consumer (свой клиент):

```go
s, err := redisqueue.NewStepsRaw(ctx, rdb, "orders", taskID)
```

После успешного `Do` в Redis пишется имя шага. При ошибке checkpoint не двигается. На **Reject** / reclaim ключ остаётся; на **Ack** и **Admin.Remove** — удаляется.

## Ручной режим

```go
s, _ := redisqueue.NewSteps(ctx, task)
_ = s.Last()
_ = s.Commit(ctx, "prepared")
```

## Release и TTL

Обычный путь Consume → Ack чистит step сам. `Release` — редко (свой цикл, сброс прогресса). TTL — страховка, если ключ забыли снять не через Ack/Remove.

## Семантика

- Checkpoint = шаг успешно завершён; при resume `Do` до этого имени включительно пропускаются.
- Имена — стабильные строки; порядок `Do` должен совпадать между попытками.
- Не exactly-once side-effect: упали после внешнего эффекта, но до `Commit` — шаг выполнится снова.

## Ключ Redis

`queue:{name}:step:{taskID}` — см. [Ключи Redis](../reference/redis-keys.md).
