# API

Пакет: `github.com/Rinsvent/smart-redis-queue`.

## Типы

`Task` — `ID`, `Partition`, `Priority`, `Payload`, `Scheduled`, `Tags`, `Groups`, `RejectCount`.

Основные объекты: `Producer`, `Consumer`, `ConsumerPool`, `Admin`.

## Producer

| Метод / опция | Назначение |
|---------------|------------|
| `NewProducer` | Создать |
| `Publish` / `PublishWith` | Публикация (батч атомарно) |
| `SetCompression` / `WithCompression` | zstd |
| `WithTagBackpressure` / `WithTagLimits` | глубина по тегу |
| `Lock` | простой distributed lock |

Ошибки: `ErrTasksAlreadyExist`, `ErrBackpressure`.

## Consumer / Pool

| Метод | Назначение |
|-------|------------|
| `Get` / `GetChan` / `Consume` | Выдача и цикл |
| `Ack` / `Reject` / `BlockGroups` | Завершение / парковка |
| `SetPollInterval` / `SetPrefetchCount` | Поллинг |
| `SetIdempotencyTtl` | Окно после Ack |
| `SetRateLimit` | Rate limit на Get |
| `SetConcurrencyTags` | `race!` |
| `SetStatsRetention` / `SetTagsStats` | Метрики |
| `SetCheckDeadConsumerLocksOnGet` | Probe dead locks |
| `Use` | Middleware |
| Pool: `SetCount` | Число воркеров |

Хелперы: `NewRejectWithDelay`, `DebounceID`, `RaceTagPrefix` и другие.

## Admin

`ListQueues`, `Inspect`, `QueueOverview`, `ListPartitions`, `ListGroups`, `ListTags`, `CountTags`, `BlockGroups`, `BlockPartition`, `UnblockPartition`, `Remove`, `RemoveByTag`, `CountByTag`, `Purge`, `Retry`, `QueueStatsBounds`, `ListQueueStats`.

Полные сигнатуры — в GoDoc и исходниках. Примеры — в [фичах](../features/index.md).
