# Journal

Внешнее хранилище (Kafka / Postgres / Mongo) — источник правды о принятой работе. Redis остаётся планировщиком: партиции, локи, rate limit.

```text
github.com/Rinsvent/smart-redis-queue/journal
github.com/Rinsvent/smart-redis-queue/journal/kafka
github.com/Rinsvent/smart-redis-queue/journal/pg
github.com/Rinsvent/smart-redis-queue/journal/mongo
```

Пример: `go run ./examples/journal` (MemSource + Bridge + Consumer).

## Как это работает

1. Пишете задачу в journal → **ready-stream** получает монотонный `journal_offset` (или Kafka offset).
2. Bridge: pump-lock → `PromoteDue` (если есть) → `ReadBatch` → `PublishWith(WithJournal)` + cursor в одном Lua.
3. Ack обработки — в journal (`acked_at` / ack-топик), не «уже прочитали».
4. После SRQ `Ack` — `Consumer.SetJournalAck`.

Курсор Redis: `srq:journal:{name}:cursor`.

| Источник | Ready offset |
|----------|----------------|
| kafka | partition + offset (`InTopic`) |
| pg / mongo | `journal_offset` (batch-reserve) |

## Почему не курсор по времени

Два продюсера / долгая подготовка → time-курсор или ObjectID «от scheduled» уезжает вперёд → skip.  
Фильтр `scheduled <= now` **в том же потоке, где двигается курсор** — та же дыра (offset 105 not-due, 106 due → cursor=106 → 105 потерян).

## Scheduled

**pg / mongo** — одна таблица: `state=scheduled` без offset → `PromoteDue` выдаёт offset на месте (`DueSkew` = 10s).

**kafka** — dual topic: due → `InTopic`, far → `ScheduleTopic`; `PromoteDue` перекладывает due в `InTopic` (новый offset). Курсор mover: `srq:journal:{name}:kafka_sched`. Без `ScheduleTopic` far уходит в `InTopic`, Redis ждёт `Scheduled`.

## Recovery

`RecoverAfterRedis`:

1. опциональный `waitAcks`;
2. **Lock** task id из `ReadAcks` (kafka ack-топик) — чтобы replay не создал дубли;
3. pump с курсора;
4. auto → `resume_after`.

pg/mongo: Lock не обязателен — `ReadBatch` режет по `acked_at`.

## Pump lock / Publish

`srq:journal:{name}:pump` — NX TTL ~60s, refresh ~10s.  
`WithJournal` в том же `publishLocal` (compression / backpressure). Шардинг: один шард на journal-батч.

## API (кратко)

```go
br := journal.NewBridge(rdb, "orders", "orders-q", src)
go br.Loop(ctx, 64, 200*time.Millisecond)

consumer.SetJournalAck(src.Ack)
consumer.SetEmergencyResume(true)
// kafka far-schedule:
// cfg.ScheduleTopic = "orders-sched"; cfg.CursorRDB, cfg.JournalName = rdb, "orders"
```

См. также: [delivery](../delivery.md#redis-persist) · [api](../reference/api.md) · [redis-keys](../reference/redis-keys.md)
