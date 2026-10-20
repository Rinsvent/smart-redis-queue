# API

Пакет: `github.com/Rinsvent/smart-redis-queue`.

## Типы

`Task` — `ID`, `Partition`, `Priority`, `Payload`, `Scheduled`, `Tags`, `Groups`, `RejectCount`.

Основные объекты: `Producer`, `Consumer`, `ConsumerPool`, `Admin`.

## Producer

| Метод / опция | Назначение |
|---------------|------------|
| `NewProducer` | Создать |
| `SetShards` / `Close` | Шардинг (тот же тип `Producer`) |
| `Publish` / `PublishWith` | Публикация (батч атомарно) |
| `PublishManual` / `PublishTo` | Явный ключ / имя шарда |
| `SetCompression` / `WithCompression` | zstd |
| `WithTagBackpressure` / `WithTagLimits` | глубина по тегу |
| `WithShardKey` / `WithShardName` | опции Publish при шардинге |
| `Lock` / `LockTo` | distributed lock |

Ошибки: `ErrTasksAlreadyExist`, `ErrBackpressure`.

## Шардинг

| Тип / метод | Назначение |
|-------------|------------|
| `ShardConfig` / `Shard` | Список `{Addr, DB}` или `Client`; `ReadOnly`, `Weight`, `KeyMap` |
| `ShardByGroup` / `ByQueue` / `ByManual` / `ByPartition` | Стратегия ключа |
| `Producer.SetShards` / `ConsumerPool.SetShards` | Включить на существующих типах |
| `ConsumerPool.SetCount` | Сумма воркеров (делятся по шардам) |

Подробности и процедура add/remove: [Шардинг](../sharding.md).

## Consumer / Pool

| Метод | Назначение |
|-------|------------|
| `Get` / `GetChan` / `Consume` | Выдача и цикл |
| `Ack` / `Reject` / `BlockGroups` | Завершение / парковка |
| `SetPollInterval` / `SetPrefetchCount` | Поллинг |
| `SetIdempotencyTtl` | Окно после Ack |
| `SetRateLimit` | Rate limit на Get |
| `SetConcurrencyTags` | `race!` |
| `NewSteps(ctx, task)` / `NewStepsRaw` / `Do` / `Commit` / `Last` / `Release` | Step recovery |
| `SetStatsRetention` / `SetTagsStats` | Метрики |
| `SetCheckDeadConsumerLocksOnGet` | Probe dead locks |
| `Use` | Middleware |
| Pool: `SetCount` | Число воркеров |

Хелперы: `NewRejectWithDelay`, `DebounceID`, `RaceTagPrefix` и другие.

## Admin

`ListQueues`, `Inspect`, `QueueOverview`, `ListPartitions`, `ListGroups`, `ListTags`, `CountTags`, `BlockGroups`, `BlockPartition`, `UnblockPartition`, `Remove`, `RemoveByTag`, `CountByTag`, `Purge`, `Retry`, `QueueStatsBounds`, `ListQueueStats`.

Полные сигнатуры — в GoDoc и исходниках. Примеры — в [фичах](../features/index.md).
