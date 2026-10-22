# Exchange / topic routing — внутренние заметки

Черновик. Архитектура ещё не зафиксирована — **в публичную доку не выносить**, пока не реализуем. См. также ROADMAP §7 (selective groups) и §8 (fan-out).

## Черновой namespace ключей (может измениться)

Глобально на инстансе (рядом с `srq:queues`):

| Ключ | Тип | Назначение |
|------|-----|------------|
| `srq:exchange:{name}` | HASH | тип exchange: `type=topic\|direct\|fanout` |
| `srq:exchange:{name}:bindings` | SET/ZSET | привязки pattern → queue; имена очередей из `srq:queues` |

После реализации — перенести описание в `docs/reference/redis-keys.md` (и фичу в `docs/features/`).
