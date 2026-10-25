# Journal

Внешнее хранилище (Kafka / Postgres / Mongo) — источник правды о принятой работе. Redis остаётся планировщиком: партиции, локи, rate limit.

```text
github.com/Rinsvent/smart-redis-queue/journal
github.com/Rinsvent/smart-redis-queue/journal/kafka
github.com/Rinsvent/smart-redis-queue/journal/pg
github.com/Rinsvent/smart-redis-queue/journal/mongo
```

## Как это работает

1. Пишете задачу в journal → **ready-stream** получает монотонный `journal_offset` (или Kafka offset).
2. Bridge: pump-lock → при необходимости `PromoteDue` → `ReadBatch` после курсора → `PublishWith(WithJournal)` + cursor в одном Lua.
3. В journal нет флага «уже прочитали». Есть **ack обработки** (`acked_at` / ack-топик).
4. После SRQ `Ack` — journal `Ack` (`Consumer.SetJournalAck`).

Курсор Redis: `srq:journal:{name}:cursor` — последний успешно уложенный **ready** offset.

| Источник | Ready offset |
|----------|----------------|
| kafka | partition + offset |
| pg / mongo | `journal_offset` (int, batch-reserve) |

## Почему не курсор по времени / ObjectID

Два продюсера или долгая подготовка: «поздний» батч уезжает в Redis раньше → time-курсор или ObjectID «от scheduled» уезжает вперёд → опоздавшее сообщение **навсегда skip**.

То же с фильтром `scheduled <= now` **в том же потоке, где двигается курсор**:

- offset 105 ещё не due, 106 due → pump забирает 106, cursor=106;
- когда 105 станет due, условие `offset > 106` его уже не видит.

Поэтому far-future **не занимает** ready-offset, пока не due.

## Scheduled без второй очереди (pg / mongo)

В той же таблице/коллекции:

- `state = scheduled` — строки **без** `journal_offset` (ещё не в ready-stream);
- `state = ready` — выдан `journal_offset`, их читает pump;
- `scheduled <= now + 10s` (`DueSkew`) при insert → сразу ready + batch-reserve offset;
- иначе → scheduled; Bridge/`PromoteDue` позже выставляет offset **на месте** (payload не копируем).

Mongo: `_id` = ObjectID (стабильный ключ документа), курсор = `journal_offset` из counter (`$inc` на весь батч → `insertMany` / bulk promote). Менять `_id` нельзя и не нужно.

Kafka так не умеет: там отдельный топик (или dual store) + mover с новым offset.

## Pump lock

`srq:journal:{name}:pump` — `SET NX`, TTL ~60s, refresh ~10s. Один writer на журнал. Ack — с любых инстансов.

## Publish

`WithJournal` — тот же `publishLocal` (compression / backpressure рядом). Rewind → `ErrJournalCursorRewind`.

## Совместимость

| Фича | Как |
|------|-----|
| compression / backpressure | `PublishOption` |
| groups / RL / `race!` | после Publish — как обычно |
| Exchange | journal → work-очередь; fan-out отдельно |
| Шардинг | cursor/lock и Publish на одном Redis; батч `WithJournal` не режется на несколько шардов |
| Emergency | `SetEmergencyResume` + `resume_after` |

## Recovery

Pump с курсора; pg/mongo — только `acked_at IS NULL` в ready. Kafka — Lock из ack-топика, затем pump. Auto-resume ставит `resume_after`.

## Схема (идея)

PG: `id` + nullable `journal_offset` + `state` + payload…  
Mongo: ObjectID `_id` + sparse unique `journal_offset` + `state`.

```go
br := journal.NewBridge(rdb, "orders", "orders-q", src)
go br.Loop(ctx, 64, 200*time.Millisecond) // внутри PromoteDue + Pump
```

См. также: [delivery](../delivery.md#redis-persist) · [api](../reference/api.md) · [redis-keys](../reference/redis-keys.md)
