# Шардинг — внутренние заметки дизайна

Публичная дока: `docs/sharding.md`. Здесь — обсуждение и решения для разработки.

## Почему не multi-DB ×N

Один `redis-server` = один main thread для Lua. `SELECT` только меняет namespace. ×N Lua = N процессов (разные `Addr`).

## Почему не Cluster в v1

Многоключевой Lua очереди (partitions, groups, rl, race) без hash-tag схемы даст CROSSSLOT / разъезд group и partition. Отдельный эпик.

## Стратегии (реализовано)

- ByQueue / ByGroup / ByManual / ByPartition
- KeyMap sticky, Weight, ReadOnly для drain
- hash: FNV-1a + weighted mod
- `Producer.SetShards` / `ConsumerPool.SetShards` (без отдельных типов)
- Publish режет батч по шарду; pool: workers ÷ shards, слушает и ReadOnly

## Миграция

v1: процедура drain + ReadOnly, без dual-read/consistent hashing. Consistent hashing — возможный follow-up.

## Бенчи (снято 2026-10-20)

Сырые: `docs/bench-results/sharding-HEAD.txt`. Публично: `docs/sharding.md`, `docs/benchmarks.md`.

- Publish 1 DB vs 4 DB same process: без выигрыша (чуть хуже из‑за router).
- Publish 4× redis-server: ~1.5× (не линейно на этой машине).
- Consume 4 DB same process: ~1.15× — меньше ключей/нагрузки на DB.

## Admin / UI

Позже: каталог = union по шардам; метрики per addr.
