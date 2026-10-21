# Ключи Redis

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
