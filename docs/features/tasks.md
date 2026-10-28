# Tasks (control-plane)

Рядом с очередями: каталог команд, cron-scheduler, last-run, pause/stop через step-ключ. Очередь — только транспорт + anti-dupe по ID.

## Регистрация

```go
reg, _ := tasks.New(rdb, tasks.Config{
    Namespace:    "billing",
    DefaultQueue: "billing-cron",
})

// разные опции / расписания = разные Name (Dispatch ищет handler по base name)
_ = reg.Register(tasks.Def{
    Name: "import_full",
    Cron: "0 3 * * *",
    Concurrency: tasks.ConcurrencyForbid,
    Args: []tasks.ArgDef{{Name: "mode", Default: "full"}},
    Handle: importHandle,
})
_ = reg.Register(tasks.Def{
    Name: "import_delta",
    Cron: "*/15 * * * *",
    Concurrency: tasks.ConcurrencyAfter,
    Args: []tasks.ArgDef{{Name: "mode", Default: "delta"}},
    Handle: importHandle, // тот же код
})

reg.SetProducer("billing-cron", producer)
_ = reg.SyncCatalog(ctx)
reg.StartScheduler(ctx)
```

Scheduler публикует payload из defaults Args. UI last-args cron не перебивают.

## Concurrency

Когда cron-тик наступил, а `payload:{name}` ещё жив:

| Режим | Поведение |
|-------|-----------|
| `forbid` | Пропустить тик |
| `after` | `pending_rerun=1`; **сразу после** окончания текущего Dispatch — новый Enqueue (не ждём следующий час крона) |
| `allow` | Тик с `~r` — параллельный прогон |

Пример `after`: cron раз в час, прогон 1.5ч → в :60 тик ставит `pending_rerun` → в :90 по End сразу стартует следующий (не в :120).

## UI Run (вручную)

В модалке по-прежнему: bypass (`~r`) и lock-by-options (`~o`) — только для ручного запуска, не в catalog/Def.

## Ключи

| Ключ | Назначение |
|------|------------|
| `srq:tasks:namespaces` | SET |
| `srq:tasks:{ns}:catalog` | cron, concurrency, args |
| `srq:tasks:{ns}:run:{name}` | status, next_run_at, pending_rerun, paused_at, progress |

См. [Step recovery](step-recovery.md).
