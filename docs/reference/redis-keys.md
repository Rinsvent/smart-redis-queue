# Ключи Redis

Префикс очереди: `queue:{name}:…`

| Ключ | Тип | Назначение |
|------|-----|------------|
| `partitions` | SET | Партиции legacy (QueuePriority=0) |
| `priorities` | ZSET | Активные QueuePriority > 0 |
| `qp{N}:partitions` | SET | Партиции lane qp=N |
| `qp{N}:partition:{p}:{pp}` | ZSET | Задачи lane qp=N |
| `qp{N}:partition:{p}:priorities` | ZSET | PartitionPriority в lane |
| `qp{N}:group:{g}:ready` | ZSET | Ready партиции группы в lane |
| `partition:{p}:{pp}` | ZSET | Ожидающие задачи (legacy) |
| `priority:{id}` | STRING | PartitionPriority задачи |
| `qpriority:{id}` | STRING | QueuePriority задачи (только если > 0) |
| `payload:{id}` | STRING | Тело задачи / маркер идемпотентности |
| `step:{id}` | STRING | Checkpoint step recovery; чистится на Ack/Remove |
| `headers:{id}` | HASH | Мета между попытками; чистится на Ack/Remove |
| `consumers` | HASH | Heartbeat consumer’ов |
| `consumer:{cid}:tasks` | SET | In-progress у consumer |
| `groups` | ZSET | Группы; score ≤ now — свободна (общее, без qp) |
| `group:{g}:ready` | ZSET / SET | Ready партиции группы (legacy) |
| `partition:{p}:groups` | SET | Группы партиции (общее) |
| `partition:{p}:lock` | STRING | Lock ordered-партиции (общее) |
| `blocked` | ZSET | Заблокированные партиции |
| `debounce:{id}` | STRING | Флаг in-flight debounce |
| `race:tags:{id}` | SET | `race!` теги задачи |
| `race:locks` | HASH | Активные concurrency locks |
| `rl:g:*` / `rl:p:*` | — | Состояние rate limit на Get |
| `tag:{tag}` | SET | Индекс задач по тегу |
| `stats:{sec}` | HASH | get/ack/reject (+ теги) |
| `stats:index` | ZSET | Индекс секунд stats |

Точные имена и Lua — в исходниках. Ключи руками лучше не трогать: используйте Admin или CLI.
