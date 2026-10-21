# Step recovery

Helper для checkpoint внутри handler: несколько неатомарных шагов переживают падение воркера без повторного выполнения уже завершённых.

После `Get` / `Consume` у `Task` уже есть bind на redis и имя очереди (в т.ч. нужный шард) — `NewSteps` берёт их оттуда.

Слои гарантий очереди (продюсер / Redis / воркер) и сравнение с Kafka — в [Гарантиях доставки](../delivery.md#guarantee-layers).

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
- Get уже отдаёт задачу одному consumer: Steps не про конкуренцию воркеров, а про **повтор во времени** после Reject/reclaim.
- Окно: упали после внешнего эффекта, но до `Commit` — этот шаг выполнится снова (как в любой at-least-once очереди). Закрывается идемпотентностью шага, не флагом библиотеки.

`!`-партиция + Steps: один обработчик, порядок, прогресс между попытками. Это каркас под «ровно один раз» по бизнесу, если каждый шаг идемпотентен.

## Идемпотентный handler {#idempotent-handler}

Цель: при повторной доставке той же задачи бизнес-эффект не дублируется.

### Upsert в своей БД

```go
s.Do(ctx, "write_db", func(ctx context.Context) error {
    // UNIQUE(task_id) или ON CONFLICT DO NOTHING / DO UPDATE
    return db.UpsertOrder(ctx, task.ID, task.Payload)
})
```

Повтор после reclaim: тот же `task.ID` → та же строка, без второго «создания».

### Внешний API со своим Idempotency-Key

Формат ключа — **контракт внешнего сервиса**, не SRQ. Библиотека ключ не генерирует и отдельных ключей в Redis под это не заводит.

```go
s.Do(ctx, "charge", func(ctx context.Context) error {
    // пример: склеить id задачи и имя шага — как удобно вашему API
    return payments.Charge(ctx, amount, payments.WithIdempotencyKey(task.ID+":charge"))
})
```

Сценарий: `Charge` OK → crash до `Commit` → reclaim → `Do("charge")` снова с тем же ключом → платёжка возвращает прежний результат, повторного списания нет → затем `Commit` и дальше.

Без поддержки ключа на стороне API этот приём не сработает — нужен другой идемпотентный контракт (или смириться с at-least-once эффекта).

### Несколько шагов

```go
pool.Consume(ctx, func(task *redisqueue.Task) error {
    s, err := redisqueue.NewSteps(ctx, task)
    if err != nil {
        return err
    }
    if err := s.Do(ctx, "prepare", func(ctx context.Context) error {
        return db.UpsertOrder(ctx, task.ID, task.Payload)
    }); err != nil {
        return err
    }
    if err := s.Do(ctx, "charge", func(ctx context.Context) error {
        return payments.Charge(ctx, amount, payments.WithIdempotencyKey(task.ID+":charge"))
    }); err != nil {
        return redisqueue.NewRejectWithDelay(err, 5.0)
    }
    return s.Do(ctx, "notify", func(ctx context.Context) error {
        return notify.Send(ctx, task.ID) // тоже лучше идемпотентно
    })
})
```

После успеха Consume делает Ack. Повторный Publish того же id при необходимости режется через `SetIdempotencyTtl` — см. [Debounce / идемпотентность](debounce-idempotency.md) и [Гарантии доставки](../delivery.md).

## Ключ Redis

`queue:{name}:step:{taskID}` — см. [Ключи Redis](../reference/redis-keys.md).
