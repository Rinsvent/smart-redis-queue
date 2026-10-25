# Journal

Внешнее хранилище (Kafka / Postgres / Mongo) — источник правды о принятой работе. Redis остаётся планировщиком: партиции, локи, rate limit.

```text
github.com/Rinsvent/smart-redis-queue/journal
github.com/Rinsvent/smart-redis-queue/journal/kafka
github.com/Rinsvent/smart-redis-queue/journal/pg
github.com/Rinsvent/smart-redis-queue/journal/mongo
```

Свой `Source` можно написать и передать в `NewBridge`. Пример: `go run ./examples/journal`.

## Как это работает

1. Пишете задачу в journal → **ready-stream** получает монотонный offset.
2. Bridge: pump-lock → `PromoteDue` (если есть) → `ReadBatch` → Publish в Redis + cursor.
3. Ack обработки — в journal (`acked_at` / ack-топик).
4. После SRQ `Ack` — `Consumer.SetJournalAck`.

Курсор Redis: `srq:journal:{name}:cursor` (всегда на meta-клиенте Bridge).

| Источник | Ready offset |
|----------|----------------|
| kafka | partition + offset (`InTopic`, **v1 = только partition 0**) |
| pg / mongo | `journal_offset` (batch-reserve) |

## Почему не курсор по времени

Два продюсера / долгая подготовка → курсор по `scheduled` или ObjectID «от времени» уезжает вперёд → сообщение навсегда skip.  
Фильтр `scheduled <= now` в том же потоке, где двигается cursor: not-due 105 и due 106 → cursor=106 → 105 потерян. Поэтому far-future не занимает ready-offset, пока не due.

## Scheduled

**pg / mongo** — одна таблица: `state=scheduled` без offset → `PromoteDue` выдаёт offset на месте (`DueSkew` = 10s).

**kafka** — dual topic: due → `InTopic`, far → `ScheduleTopic`; `PromoteDue` копирует due в `InTopic`. Курсор mover: `srq:journal:{name}:kafka_sched`. Без `ScheduleTopic` far идёт в `InTopic`, Redis ждёт `Scheduled`.

## Шардинг

Journal (kafka/pg/mongo) один; в Redis очередь может быть sharded.

```go
br := journal.NewBridge(metaRedis, "orders", "orders-q", src)
_ = br.SetShards(cfg) // тот же ShardConfig, что у ConsumerPool
```

- **meta `rdb`** (аргумент `NewBridge`) — pump-lock и journal cursor.
- **SetShards** — задачи раскладываются по шардам как у обычного Producer (`ShardByGroup` и т.д.).
- Cursor **не** дублируется на каждый шард: Publish на шарды, затем SET cursor на meta. Обрыв между фазами безопасен (replay + NX).

Прямой `PublishWith(WithJournal)` на несколько шардов по-прежнему запрещён (атомарный Lua на одном Redis). Bridge при шардинге этот путь не использует.

## Exchange (как стыковать)

Сейчас Bridge пишет через **Producer** в одну logical queue. Exchange — отдельный маршрутизатор (копии в N очередей).

Чтобы journal кормил Exchange:

1. В записи journal хранить **routing key** (поле в payload / envelope / своё поле в `Source`).
2. В Bridge вместо `Producer.PublishWith` вызывать `Exchange.PublishWith(kind, routingKey, …)` на каждую задачу или батч с одним key.
3. Cursor по-прежнему на meta Redis **после** успешного publish (как при шардинге): у Exchange нет `WithJournal` в Lua `ex`.

Черновик API (ещё не в коде): `Bridge.SetExchange(ex, kind, keyFn)` + commit cursor на meta. Пока можно сделать тонкую обёртку у себя: свой `Source` + цикл ReadBatch → Exchange.Publish → SET cursor.

## Recovery

`RecoverAfterRedis`: waitAcks → Lock id из `ReadAcks` (kafka; на всех шардах если SetShards) → pump → `resume_after`.  
pg/mongo: Lock не нужен — `ReadBatch` режет по `acked_at`.

## Pump lock / Publish

`srq:journal:{name}:pump` — NX TTL ~60s, refresh ~10s.  
Single-Redis: `WithJournal` в том же Lua, что Publish.  
Compression / backpressure — `SetPublishOptions`.

```go
br := journal.NewBridge(rdb, "orders", "orders-q", src)
_ = br.SetShards(cfg) // опционально
go br.Loop(ctx, 64, 200*time.Millisecond)

consumer.SetJournalAck(src.Ack)
consumer.SetEmergencyResume(true)
```

См. также: [delivery](../delivery.md#redis-persist) · [api](../reference/api.md) · [sharding](../sharding.md) · [redis-keys](../reference/redis-keys.md)
