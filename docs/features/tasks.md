# Tasks (control-plane)

Рядом с очередями: каталог команд, cron-scheduler, last-run, pause/stop через step-ключ. Очередь — только транспорт + anti-dupe по ID.

## Минимальный воркер (UI + cron)

Scheduler и UI только **публикуют** в очередь. Чтобы Run из UI что-то сделал, нужен процесс с `Register` + `Consume`:

```go
reg, _ := tasks.New(rdb, tasks.Config{
    Namespace:    "billing",
    DefaultQueue: "billing-cron",
})

_ = reg.Register(tasks.Def{
    Name:        "import_full",
    Cron:        "0 3 * * *",
    Concurrency: tasks.ConcurrencyForbid,
    Args:        []tasks.ArgDef{{Name: "mode", Default: "full"}},
    Handle:      importHandle,
})
_ = reg.Register(tasks.Def{
    Name:        "import_delta",
    Cron:        "*/15 * * * *",
    Concurrency: tasks.ConcurrencyAfter,
    Args:        []tasks.ArgDef{{Name: "mode", Default: "delta"}},
    Handle:      importHandle, // тот же код, другой Name
})

_ = reg.SyncCatalog(ctx) // UI увидит catalog
reg.StartScheduler(ctx)  // опционально; без Consume тики только копятся в очереди

// блокирует до cancel: пул на каждую очередь из Def / DefaultQueue
err := reg.Consume(ctx, tasks.ConsumeOptions{Workers: 8})
```

`SetProducer` не обязателен — `Enqueue` / `Consume` создают Producer на `rdb` registry сами.

Поток: **UI Run / Scheduler** → `Publish` (тот же task id / payload-лок) → **воркер** `Get` → `Dispatch` по `BaseName(task.ID)` → handler из `Def`.

## Почему пул, а не consumer на задачу

| Вариант | Минус |
|---------|--------|
| Consumer на каждый Name / при появлении задачи | Лишние poll-циклы, сложно масштабировать, нет выигрыша vs пула |
| Один воркер на очередь | UI Run ждёт, пока длинный cron освободит слот |
| **Пул воркеров на очередь** (`Consume`) | Параллельные Name / `~r` сразу; лок только NX payload |

Лок из UI — обычный Publish SET NX по id. Нет лока → задача берётся свободным воркером сразу. Несколько `Def` на одной очереди не мешают друг другу, пока хватает `Workers`.

Если очередей несколько (`Def.Queue` разный) — `Consume` поднимает **отдельный пул на каждую**.

Ручной путь без registry (редко): свой `ConsumerPool` + `reg.Dispatch(ctx, task)`.

## Concurrency

Когда cron-тик наступил, а `payload:{name}` ещё жив:

| Режим | Поведение |
|-------|-----------|
| `forbid` | Пропустить тик |
| `after` | `pending_rerun=1`; новый Enqueue **тем же id** после того, как текущий прогон **Ack**’нут (не ждём следующий cron) |
| `allow` | Тик с `~r` — параллельный прогон |

Пока задача in-flight, `payload:{name}` жив → Enqueue NX не проходит, флаг **не** снимается. `Registry.Consume` вызывает flush в `SetAfterAck`; scheduler тоже пробует каждый тик.

Пример `after`: cron раз в час, прогон 1.5ч → в :60 тик ставит `pending_rerun` → в :90 после Ack сразу стартует следующий (не в :120).

## UI Run (вручную)

В модалке: bypass (`~r`) и lock-by-options (`~o`) — только для ручного запуска, не в catalog/Def. Оба пишут в ту же очередь, что обрабатывает `Consume`.

## Ключи

| Ключ | Назначение |
|------|------------|
| `srq:tasks:namespaces` | SET |
| `srq:tasks:{ns}:catalog` | cron, concurrency, args |
| `srq:tasks:{ns}:run:{name}` | status, next_run_at, pending_rerun, paused_at, progress |

См. [Step recovery](step-recovery.md).
