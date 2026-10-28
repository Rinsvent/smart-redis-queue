# Tasks (control-plane)

Рядом с очередями: каталог команд, cron-scheduler, last-run, pause/stop через step-ключ. Очередь — только транспорт + anti-dupe по ID.

## Идея

| | Queue + Steps | Tasks |
|--|---------------|--------|
| Доставка / lock ID | да | — |
| Progress mid-flight | `step:{id}` | зеркало в `run` HASH |
| Каталог / args / cron | — | да |
| Last-run после Ack | step удаляется | `srq:tasks:{ns}:run:{name}` |

## Регистрация

```go
reg, _ := tasks.New(rdb, tasks.Config{
    Namespace:    "billing",
    DefaultQueue: "billing-cron",
})

// две «команды» с разными опциями = два Def (разные Name/Args + ByOptions)
_ = reg.Register(tasks.Def{
    Name: "import_full",
    Cron: "0 3 * * *",
    Concurrency: tasks.ConcurrencyForbid, // skip если ещё бежит
    ByOptions: true,
    Args: []tasks.ArgDef{{Name: "mode", Default: "full"}},
    Handle: importHandle,
})
_ = reg.Register(tasks.Def{
    Name: "import_delta",
    Cron: "*/15 * * * *",
    Concurrency: tasks.ConcurrencyAfter, // если тик при живом локе — повтор после End
    ByOptions: true,
    Args: []tasks.ArgDef{{Name: "mode", Default: "delta"}},
    Handle: importHandle,
})

reg.SetProducer("billing-cron", producer)
_ = reg.SyncCatalog(ctx)
reg.StartScheduler(ctx)
// pool.Consume → reg.Dispatch(ctx, task)
```

Scheduler шлёт payload **только из defaults** схемы Args. UI last-args cron не перебивают.

## Concurrency (когда тик наступил, а задача ещё жива)

| Режим | Поведение |
|-------|-----------|
| `forbid` (default) | Пропустить тик (лок payload) |
| `after` | `pending_rerun=1` → после Dispatch Enqueue ещё раз |
| `allow` | Тик с `~r` (параллельный прогон) |

**Почему не debounce:** `deb!` = перезапись payload in-flight и повтор после Ack с *новым* телом (latest-wins). `after` = один отложенный прогон теми же defaults schedule, без параллели и без смены payload.

## UI Run

- `ID=Name` — обычный лок
- **By options** → `Name~o{hash8}`
- **Bypass lock** → `Name~r{rand}` (by options тогда disabled)

## Progress / control

Счётчик и `!pause`/`!stop` в `step:{id}`. Flush — Lua, один RTT. `paused_at` в run HASH для UI (>15m).

## Ключи

| Ключ | Назначение |
|------|------------|
| `srq:tasks:namespaces` | SET namespace |
| `srq:tasks:{ns}:catalog` | HASH name → JSON (cron, concurrency, by_options, bypass_lock, args) |
| `srq:tasks:{ns}:run:{name}` | status, next_run_at, pending_rerun, paused_at, progress, … |

См. также [Step recovery](step-recovery.md).
