# API

Пакет: `github.com/Rinsvent/smart-redis-queue`.

## Типы

`Task` — `ID`, `Partition`, `PartitionPriority`, `QueuePriority`, `Payload`, `Scheduled`, `Tags`, `Groups`, `RejectCount`.

Основные объекты: `Producer`, `Consumer`, `ConsumerPool`, `Exchange`, `Admin`.

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

## Exchange

| Метод / опция | Назначение |
|---------------|------------|
| `NewExchange` | Имя + Redis; конфиг сам не пишет |
| `Bind` / `Unbind` / `SetBindings` / `Bindings` | Локальные bindings |
| `Persist` / `Load` / `Version` | Явная запись / чтение bindings+ver |
| `Publish` / `PublishWith` | kind + routingKey → matched queues |
| `ExchangeDirect` / `Fanout` / `Topic` | Алгоритм матчинга (не в Redis) |
| `WithDeliveryAtomic` | Один EVAL на все dest (default) |
| `WithDeliveryBestEffort` | N× Publish |
| `WithCheckVersion` | Opt-in GET ver + reload при mismatch |
| `SetCompression` / `WithExchangeCompression` | zstd |
| `WithExchangeTagBackpressure` | backpressure на dest до записи |

Ошибки: `ErrNoRoute`, `ErrExchangeVersionConflict`, `ErrBackpressure`.

Подробности: [Exchange](../features/exchange.md).

## Journal

Пакеты `journal`, `journal/kafka`, `journal/pg`, `journal/mongo` (import только нужный драйвер).

| API | Назначение |
|-----|------------|
| `journal.Bridge` / `Pump` / `Loop` / `RecoverAfterRedis` | journal→Redis; recovery Lock из `ReadAcks` + pump |
| `Bridge.SetShards` / `SetProducer` / `SetPublishOptions` | шардинг (cursor на meta rdb); compression / backpressure |
| `Producer.LockOnAllShards` | recovery Lock id на каждом шарде |
| `DuePromoter` / `PromoteDue` | scheduled→ready (pg/mongo in-place; kafka ScheduleTopic→InTopic) |
| `journal.DueSkew` / `IsDue` | окно due (10s) |
| `WithJournal(name, cursor)` | PublishOption: задачи + cursor (иначе `ErrJournalCursorRewind`) |
| `SetResumeAfter` / `GetResumeAfter` | emergency stamp |
| `Consumer.SetJournalAck` | после SRQ Ack |
| `Consumer.SetAfterAck` | после SRQ Ack (после journalAck) |
| `Consumer.SetEmergencyResume` | ждать stamp после fault |
| `kafka.Source.Publish` | due→InTopic, far→ScheduleTopic |

`WithJournal` нельзя смешивать с батчем на несколько шардов. Подробности: [Journal](../features/journal.md).

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
| `SetSamePartitionBatch` | Пачка `Get` из одной партиции |
| `SetExtendGroupBlocksOnly` | Пауза группы только продлевается (`ZADD GT`) |
| `SetIdempotencyTtl` | Окно после Ack |
| `SetRateLimit` | Rate limit на Get |
| `SetConcurrencyTags` | `race!` / `race!s!` |
| `Ack` / `WithIdempotency` / `WithShared` / `WithSharedTTL` | завершение + optional Shared |
| `NewShared` / `Shared.Publish` | mid-handler prepare-once blob + unlock `race!s!` |
| `RaceSharedTag` / `Task.Shared` / `Task.SetAckShared` | shared id / Get blob / Consume→Ack |
| `NewSteps` / `NewStepsRaw` / `Do` / `Begin` / `End` / `Progress` / `Commit` / `Last` / `LastProgress` / `LastCursor` / `Release` / `SetStepControl` / `ParseStepCheckpoint` | Step recovery + progress (`name#3/?:cur@ms[!pause]`); см. [доку](../features/step-recovery.md) |
| `Task.Headers` / `SetHeader` / `SetHeaders` | Мета между попытками |
| `SetStatsRetention` / `SetTagsStats` | Метрики |
| `SetCheckDeadConsumerLocksOnGet` | Probe dead locks |
| `Use` | Middleware |
| Pool: `SetCount` | Число воркеров |

Хелперы: `NewRejectWithDelay`, `DebounceID`, `RaceTagPrefix` и другие.

## Admin

`ListQueues`, `ListQueueIndex`, `QueueDepth`, `QueueDepthIfIndexed`, `RebuildQueueDepth`, `Inspect`, `QueueOverview`, `ListPartitions`, `ListBlockedPartitions`, `ListLockedPartitions`, `ListGroups`, `ListBlockedGroups`, `ListRaceLocks`, `ScanTopPending`, `ScanHighReject`, `ListTags`, `CountTags`, `ListMessages`, `InspectTask`, `SetTaskStepControl`, `BlockGroups`, `BlockPartition`, `UnblockPartition`, `Remove`, `RemoveByTag`, `CountByTag`, `Purge`, `Retry`, `QueueStatsBounds`, `ListQueueStats`.

Пакет `tasks`: `New`, `Register`, `SyncCatalog`, `Dispatch`, `Enqueue`, `Consume`, `StartScheduler`, `ListForQueue` — см. [Tasks](../features/tasks.md).

`srq:queues` — total очереди без SCAN партиций; UI сортирует alpha или по total. `QueueDepthIfIndexed` — ok=false если реестра/члена ещё нет (график total пропускает). Top pending / poison / locked — фоновые SCAN в Admin/UI, не hot path.

Полные сигнатуры — в GoDoc и исходниках. Примеры — в [фичах](../features/index.md).
