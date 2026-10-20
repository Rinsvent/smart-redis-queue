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

## Бенчи (план)

- A: 1 Redis × N DB — ожидание ≈ без выигрыша Lua
- B: N redis-server × ByGroup — ожидание порядка ×N до упора в CPU/сеть

## Admin / UI

Позже: каталог = union по шардам; метрики per addr.
