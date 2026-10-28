# Tasks (control-plane)

Рядом с очередями: каталог команд, scheduler из конфига, last-run, pause/stop через step-ключ. Очередь — только транспорт + anti-dupe по ID.

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
    DefaultQueue: "billing-cron", // не писать Queue в каждом Def
})
_ = reg.Register(tasks.Def{
    Name:     "import",
    Interval: time.Hour, // 0 = только вручную из UI
    Args:     []tasks.ArgDef{{Name: "day", Default: "1"}},
    Handle: func(ctx context.Context, inv tasks.Invocation) error {
        // Steps + Progress; TrackStep после Advance при желании
        return nil
    },
})
reg.SetProducer("billing-cron", producer)
_ = reg.SyncCatalog(ctx)
reg.StartScheduler(ctx)
// pool.Consume → reg.Dispatch(ctx, task)
```

Scheduler шлёт payload **только из defaults схемы** — last args из UI cron не перебивают.

## Ключи

| Ключ | Назначение |
|------|------------|
| `srq:tasks:{ns}:catalog` | HASH name → JSON схемы (queue, interval_sec, args) |
| `srq:tasks:{ns}:run:{name}` | last-run: status, started_at, finished_at, progress, last_error |

Отдельный `names` SET не нужен — `HKEYS catalog`.

## UI Run

- По умолчанию `ID=Name` → лок payload.
- **By options** → `Name~o{hash8}` — одинаковые args блокируются, другие наборы можно параллелить.
- **Bypass lock** → `Name~r{rand}` — всегда новый ID (галочка by options тогда не нужна).
- Режим **tasks** в Manager: Def’ы текущей очереди, статусы, replay, pause/stop.

## Progress / control

Счётчик и `!pause`/`!stop` живут в одном `step:{id}`. Flush — Lua: пишет progress, **не затирает** control от UI, один RTT. Отдельный ключ control не нужен (Ack и так чистит step).

## Control

В step: `name#curr/total@ms!pause` / `!stop`. Admin `SetTaskStepControl`. Воркер в `Advance` синхронизирует control (SET GET) и при pause ждёт.

См. также [Step recovery](step-recovery.md), [Messages peek](../reference/api.md).
