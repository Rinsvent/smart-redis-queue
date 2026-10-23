# Exchange / topic routing — внутренний дизайн

Публично: [`docs/features/exchange.md`](../../docs/features/exchange.md). Здесь — краткие решения для разработки. ROADMAP §8 (fan-out), не §7 (selective groups).

## Решения

- **Тип exchange не в Redis** — `direct` / `fanout` / `topic` только алгоритм матчинга на продюсере.
- **Bindings** — стабильные имена очередей (как Rabbit). Consumer = обычный `NewConsumer` на очередь из кода; пусто → sleep.
- **Без EnsureQueue** — ключи очереди создаёт `add_tasks` при первой записи.
- **Матчинг в Go**; Redis не гоняет topic-regex.
- **Persist явный** (`Exchange.Persist`) — CI / init / admin; не из NewProducer/Consumer.
- **Atomic multi-dest** — один EVAL, `add_tasks` на каждый dest (копии атомарны).
- **`checkVersion` opt-in** — Go шлёт `ver` в ARGV только с `WithCheckVersion()`; иначе Lua без `GET …:ver`. Статичные bindings → без GET на hot path.
- **Best-effort** — N× single-queue (частичные копии возможны); local-only / тесты.

## Ключи

| Ключ | Тип | Назначение |
|------|-----|------------|
| `srq:exchange:{name}:bindings` | HASH | field `{pattern}\x1f{queue}` → `1` |
| `srq:exchange:{name}:ver` | STRING | `INCR` на Persist; `GET` только при checkVersion |

## Lua

Один `addScript`: тело в `local function add_tasks_to_queue(...)`.

- Single: `ARGV[1]=queueName` — как раньше (Producer.Publish без изменений ARGV layout).
- Exchange: `ARGV[1]=ex`, name, versionOrEmpty, destCount, dests…, затем taskCount/bp/tasks как у single.
  - version непустой → `GET srq:exchange:{name}:ver`, mismatch → `ERR_EXCHANGE_VERSION`.
  - иначе сразу multi-add.

Go на `ERR_EXCHANGE_VERSION`: Load → rematch → retry (~3).

## API (кратко)

`NewExchange` → `Bind` / `Unbind` / `Persist` / `Load` → `Publish(kind, routingKey, tasks…)`  
Опции: `WithDeliveryAtomic` (default), `WithDeliveryBestEffort`, `WithCheckVersion`.

Пустой match → `ErrNoRoute`.
