# Ключи Redis

Глобально на инстансе (не под `queue:`):

| Ключ | Тип | Назначение |
|------|-----|------------|
| `srq:queues` | ZSET | Каталог очередей; score = **total** (ready + in-progress). Publish `+N`, Ack/Remove `−N`. Ensure при NewProducer/Consumer. |
| `srq:exchange:{name}:bindings` | HASH | Bindings Exchange: field = pattern + sep + queue. Явный `Persist`. |
| `srq:exchange:{name}:ver` | STRING | Версия bindings (`INCR` на Persist). Читается только при `WithCheckVersion`. |
| `srq:journal:{name}:cursor` | STRING | Offset journal bridge («уже в Redis»). Двигается атомарно с batch Publish. |
| `srq:journal:{name}:pump` | STRING | Single-writer lock pump (NX + TTL ~60s, refresh ~10s). |
| `srq:journal:{name}:kafka_sched` | STRING | Offset schedule-mover (kafka ScheduleTopic → InTopic). |
| `srq:resume_after` | STRING | Unix timestamp; emergency pause воркеров до bump. См. [Journal](../features/journal.md). |

Тип exchange (direct/fanout/topic) в Redis **не** хранится — алгоритм на продюсере. См. [Exchange](../features/exchange.md).

| Ключ | Тип | Назначение |
|------|-----|------------|
| `srq:tasks:namespaces` | SET | Зарегистрированные namespace (SyncCatalog) |
| `srq:tasks:{ns}:catalog` | HASH | Схема Tasks (name → JSON); см. [Tasks](../features/tasks.md) |
| `srq:tasks:{ns}:run:{name}` | HASH | Last-run status / progress mirror |

Префикс очереди: `queue:{name}:…`

| Ключ | Тип | Назначение |
|------|-----|------------|
| `partitions` | SET | Партиции legacy (QueuePriority=0) |
| `priorities` | ZSET | Активные QueuePriority > 0 |
| `qp{N}:partitions` | SET | Партиции lane qp=N |
| `qp{N}:partition:{part}:{pp}` | ZSET | Задачи lane qp=N |
| `qp{N}:partition:{part}:priorities` | ZSET | PartitionPriority в lane |
| `qp{N}:group:{g}:ready` | ZSET | Ready партиции группы в lane |
| `partition:{part}:{pp}` | ZSET | Ожидающие задачи (legacy; score = scheduled ms) |
| `partition:{part}:priorities` | ZSET | PartitionPriority для партиции (legacy) |
| `priority:{id}` | STRING | PartitionPriority задачи |
| `qpriority:{id}` | STRING | QueuePriority задачи (только если > 0) |
| `payload:{id}` | STRING | Тело задачи / маркер идемпотентности |
| `step:{id}` | STRING | Checkpoint: `name` или `name#curr/total[:cursor]@startedMs[!pause\|!stop]`; чистится на Ack/Remove |
| `headers:{id}` | HASH | Мета между попытками; чистится на Ack/Remove |
| `consumers` | SET | ID consumer’ов |
| `consumer:{cid}:tasks` | HASH | In-progress: taskId → score |
| `groups` | ZSET | Группы; score ≤ now — свободна |
| `group:{g}:ready` | ZSET / SET | Ready партиции группы |
| `blocked` | ZSET | Заблокированные партиции |
| `debounce:{id}` | STRING | Флаг in-flight debounce |
| `race:tags:{id}` | SET | `race!` / `race!s!` теги задачи |
| `race:locks` | HASH | Активные concurrency locks |
| `shared:{id}` | STRING | Prepare-once blob для `race!s!{id}` (+ EXPIRE) |
| `block:{id}` | STRING | Wait-gate `race!b!{id}`: wait / `ready` (+ EXPIRE) |
| `rl:g:*` / `rl:p:*` | — | Состояние rate limit на Get |
| `tag:{tag}` | SET | Индекс задач по тегу |
| `dlq` | ZSET | Dead letter; score = ms попадания. Reject `WithDeadLetter`; meta не чистим |
| `dlq:score:{id}` | STRING | Исходный ZSET score для RetryDLQ |
| `dlq:max` | STRING | Ёмкость DLQ (N>0); Reject trim + ping дочистка. Нет ключа / 0 → без лимита |
| `stats:{sec}` | HASH | get/ack/reject (+ теги) |
| `stats:index` | ZSET | Индекс секунд stats |

Точные имена и Lua — в исходниках. Ключи руками лучше не трогать: используйте Admin или CLI.
