# Ключи Redis

Префикс очереди: `queue:{name}:…`

| Ключ | Тип | Назначение |
|------|-----|------------|
| `partitions` | SET | Известные партиции |
| `partition:{p}` | ZSET | Ожидающие задачи (score = schedule/priority) |
| `payload:{id}` | STRING | Тело задачи / idempotency marker |
| `consumers` | HASH | Heartbeat consumer’ов |
| `consumer:{cid}:tasks` | SET | In-progress у consumer |
| `groups` | ZSET | Группы; score ≤ now = свободна |
| `group:{g}:ready` | ZSET / SET | Ready партиции группы |
| `blocked` | ZSET | Заблокированные партиции (unlockAt) |
| `debounce:{id}` | STRING | Флаг in-flight debounce |
| `race:tags:{id}` | SET | `race!` теги задачи |
| `race:locks` | HASH | Активные concurrency locks |
| `rl:g:*` / `rl:p:*` | STRING/HASH | Состояние Get rate limit |
| `tag:{tag}` | SET | Индекс задач по тегу |
| `stats:{sec}` | HASH | get/ack/reject (+ t:tag) |
| `stats:index` | ZSET | Индекс секунд stats |

Точные имена и Lua — в исходниках. Менять ключи вручную опасно: используйте Admin / CLI.
