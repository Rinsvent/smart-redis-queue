# Script / queue flags — research (не реализовано)

Статус: **ресерч**, в бэклоге. Публично: пункт в [`docs/roadmap.md`](../../docs/roadmap.md) / [`ROADMAP.md`](../../ROADMAP.md).  
Связанный разбор: canvas `flags-bitmask-analysis` (сессия), инвентарь ARGV в `scripts.go`.

## Зачем

1. Сейчас mode Publish — длинные маркеры `ARGV[1]`: `__srq_ex` / `__srq_jn` vs имя очереди. Хотелось бы короткий int-флаг вместо 9-символьной строки.
2. Bool-опции размазаны по ARGV как `"1"`/`"0"`; при новых флагах растёт число аргументов.
3. На Get часто есть ветки (race, qp lanes, ordered locks), которые для конкретной очереди никогда не нужны — лишние `redis.call`.

**Не цель этого эпика:** «настоящая» композиция Exchange∩Journal в одном Publish (оба режима сразу пишут и cursor, и multi-dest). Exchange и Journal остаются разными путями; флаги только выбирают ветку.

## Два уровня флагов

| Уровень | Где живёт | Кто пишет | Зачем |
|---------|-----------|-----------|--------|
| **L1 — system / queue caps** | `queue:{name}:flags` (STRING/INT bitmask) | Publish (OR по полям задач); clear — sticky / Admin rebuild (см. ниже) | Get/Ack/Reject: заходить ли в ветку кода, чтобы не делать лишних запросов |
| **L2 — script / call flags** | один int в ARGV скрипта | Go-клиент (Producer/Consumer/Admin) | Поведение *этого* вызова: exchange? journal? samePartition? deadLetter? … |

L1 — про **состояние очереди** («в данных есть race / qp>0 / ordered»).  
L2 — про **намерение вызова** («этот Publish — через exchange»).

Их нельзя смешивать: `samePartition` / RL / `checkDead` — per-consumer; в L1 класть нельзя.

## L2: call flags (клиент → Lua)

### Mode Publish (замена `__srq_*`)

Биты (черновик нумерации):

| Бит | Имя | Смысл |
|-----|-----|--------|
| 0 | `MODE_EX` | Exchange multi-dest layout |
| 1 | `MODE_JN` | Journal: SET cursor + single queue |

**Приоритет веток** (зафиксировано):

1. если `MODE_EX` → exchange (даже если `MODE_JN` тоже выставлен);
2. иначе если `MODE_JN` → journal;
3. иначе → обычный Publish (`ARGV` начинается с queue name / или queue в фиксированном слоте после flags).

Так длинный ключ `__srq_ex` / `__srq_jn` уходит; композиция «оба бита» = **не** «сделай оба», а «EX побеждает». Одновременный journal+exchange — вне скоупа (отдельный wire, если когда-нибудь понадобится).

Wire-набросок addScript:

```text
ARGV[1] = callFlags   -- int
-- далее layout зависит от mode:
--   EX:  exchangeName, ver, destCount, dests…, taskCount, bp…, tasks…
--   JN:  cursorKey, newCursor, queueName, taskCount, bp…, tasks…
--   normal: queueName, taskCount, bp…, tasks…
```

Либо queueName всегда в фиксированном слоте — уточнить при реализации (меньше ветвлений в Go).

### Прочие bool вызова

Тот же L2-int (другие биты) или отдельный int на Get/Ack/Reject:

| Бит (пример) | Скрипты | Сейчас |
|--------------|---------|--------|
| concurrencyTags | Get, Ack, Reject, Share | `"1"`/`"0"` |
| samePartition | Get | `"1"`/`"0"` |
| checkDeadConsumerLocks | Get | `"1"`/`"0"` |
| extendGroupBlocksOnly | Reject, BlockGroups | `"1"`/`"0"` |
| deadLetter | Reject | `"1"`/`"0"` |
| returnPayload | RemoveByTag | `"1"`/`"0"` |

`bit.band` в Redis Lua 5.1 (`bit.*`) — дёшево; профит L2 сам по себе в основном **стабильный ARGV** и короче mode-маркеры, не p99 Get.

## L1: system flags (очередь)

Ключ: `queue:{name}:flags`. Publish при add задач: `flags |= bit` по фактическим полям.

| Бит (кандидат) | Условие Publish | Что Get может не делать |
|----------------|-----------------|-------------------------|
| `HAS_RACE` | есть tag `race!` / `race!s!` / `race!b!` | ветка `tryAcquireRace`, HGETALL locks |
| `HAS_QP` | `queuePriority > 0` | `ZREVRANGE priorities` + цикл qp lanes |
| `HAS_ORDERED` | partition с префиксом `!` | probe lock/count |
| `HAS_PP` | `partitionPriority != 0` | слабый выигрыш |
| `HAS_DEBOUNCE` | id `deb!` | почти ничего на Get |
| `HAS_GROUPS_CUSTOM` | groups ≠ только `_default` | слабый / сомнительный |

Самый ценный — **HAS_RACE** (и частично HAS_QP / HAS_ORDERED). Пустой `ZREVRANGE priorities` уже дёшев; «подставить 0» — микро-оптимизация.

### Жизненный цикл L1

| Стратегия | Clear | Риск |
|-----------|-------|------|
| **Sticky OR** (рекомендация v1) | только Admin.RebuildFlags / полный Purge | биты липнут → лишний оверхед, но не баг корректности |
| Refcount | ±1 на Publish/Ack/Reject/Remove/DLQ/Retry/reclaim | точность; легко разъехаться по путям |
| «Обнулить когда depth=0» | по `srq:queues` | **опасно**: in-progress, DLQ, debounce reactivate ≠ depth |

**False clear HAS_RACE → баг корректности** (два consumer на один ресурс). False keep → только perf. Поэтому v1 = sticky OR.

### SetConcurrencyTags

Сейчас: индекс race пишется на Publish всегда; проверка в Get — только opt-in.  
Если L1 `HAS_RACE` начнёт сам включать ветку — меняется контракт README. Нужен override: `force off` / явный `SetConcurrencyTags(false)` важнее caps, или caps только hint при `auto`.

## Инвентарь «как сейчас» (кратко)

**Mode markers:** `__srq_ex`, `__srq_jn` в `addScript` (`exchange.go` / `journal_publish.go`).

**Bool ARGV:** Get (checkDead, concurrencyTags, samePartition + rlAlgo), Ack/Share (concurrencyTags), Reject (concurrencyTags, extendOnly, deadLetter), BlockGroups (extendOnly), RemoveByTag (returnPayload).

**Уже data-driven без флага:** race tags, qp/pp, groups, debounce prefix.

## Плюсы / минусы (сводка)

**Плюсы:** короткий mode вместо `__srq_*`; стабильный префикс ARGV; L1 реально режет Redis I/O на Get; единый язык битов в Go (`const FlagEx = 1 << iota`).

**Минусы:** opaque в redis-cli; жёсткая нумерация битов; L1 clear сложен; смена семантики concurrency opt-in; миграция EVALSHA/клиентов.

## Вне скоупа / не делать в первой итерации

- Одновременный Exchange+Journal как один атомарный Publish.
- Auto-clear L1 «когда всё разобрано» без полного аудита путей.
- Класть consumer-local опции в `queue:{name}:flags`.

## Критерии готовности (когда решим делать)

1. Зафиксировать битовые константы L1/L2 в одном Go-файле + комментарии в Lua.
2. addScript: приоритет EX > JN > normal на L2; тесты exchange / journal / plain.
3. Get: ранний skip по L1 sticky; RebuildFlags + тест «после Purge биты 0».
4. Документация: redis-keys, api, контракт concurrency vs HAS_RACE.
5. Бенч: очередь без race (L1=0) vs сегодня с `SetConcurrencyTags(false)` — ожидание паритет; с race — без регрессии.
