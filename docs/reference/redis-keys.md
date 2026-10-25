# Ключи Redis

Глобально на инстансе (не под `queue:`):

| Ключ | Тип | Назначение |
|------|-----|------------|
| `srq:queues` | ZSET | Каталог очередей; score = **total** (ready + in-progress). Publish `+N`, Ack/Remove `−N`. Ensure при NewProducer/Consumer. |
| `srq:exchange:{name}:bindings` | HASH | Bindings Exchange: field = pattern + sep + queue. Явный `Persist`. |
| `srq:exchange:{name}:ver` | STRING | Версия bindings (`INCR` на Persist). Читается только при `WithCheckVersion`. |
| `srq:journal:{name}:cursor` | STRING | Offset journal bridge («уже в Redis»). Двигается атомарно с batch Publish. |
| `srq:journal:{name}:pump` | STRING | Single-writer lock pump (NX + TTL ~60s, refresh ~10s). |
| `srq:resume_after` | STRING | Unix timestamp; emergency pause воркеров до bump. См. [Journal](../features/journal.md). |

Тип exchange (direct/fanout/topic) в Redis **не** хранится — алгоритм на продюсере. См. [Exchange](../features/exchange.md).

Префикс очереди: `queue:{name}:…`

| Ключ | Тип | Назначение |
|------|-----|------------|
| `partitions` | SET | Известные партиции |
| `partition:{p}` | ZSET | Ожидающие задачи |
| `payload:{id}` | STRING | Тело задачи / маркер идемпотентности |
| `step:{id}` | STRING | Checkpoint step recovery; чистится на Ack/Remove |
| `headers:{id}` | HASH | Мета между попытками; чистится на Ack/Remove |
| `consumers` | HASH | Heartbeat consumer’ов |
| `consumer:{cid}:tasks` | SET | In-progress у consumer |
| `groups` | ZSET | Группы; score ≤ now — свободна |
| `group:{g}:ready` | ZSET / SET | Ready партиции группы |
| `blocked` | ZSET | Заблокированные партиции |
| `debounce:{id}` | STRING | Флаг in-flight debounce |
| `race:tags:{id}` | SET | `race!` теги задачи |
| `race:locks` | HASH | Активные concurrency locks |
| `rl:g:*` / `rl:p:*` | — | Состояние rate limit на Get |
| `tag:{tag}` | SET | Индекс задач по тегу |
| `stats:{sec}` | HASH | get/ack/reject (+ теги) |
| `stats:index` | ZSET | Индекс секунд stats |

Точные имена и Lua — в исходниках. Ключи руками лучше не трогать: используйте Admin или CLI.
