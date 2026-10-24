# Journal — внутренние заметки

Публично: [`docs/features/journal.md`](../../docs/features/journal.md). Здесь — связь с Requester и детали реализации.

## Опыт Requester

Сервис [Rinsvent/requester](https://github.com/Rinsvent/requester) / [хабр](https://habr.com/ru/articles/1028010/):

- Kafka `in` → router → Redis (если due ≤ ~1s) или PostgreSQL outbox (дальше).
- PG mover → снова Kafka `in` → Redis.
- Workers → Kafka `out`.
- Recovery: `OutRelockConsumer` читает `out` с offset и делает `Producer.Lock(taskID)` перед повторной вычиткой.
- В Redis часто **offset-ref** вместо полного payload (`OffsetRef` + `Fetcher`).

Публичную доку SRQ **не** ссылаем на Requester; паттерны переносим обобщённо.

## Отличия SRQ journal от «commit offset после Ack»

В Requester Kafka `in` часто **1 partition** + быстрый путь в Redis; всё же задача может задерживаться. В SRQ логические партиции блокируются независимо → **нельзя** отождествлять Kafka consumer lag с «ещё не Ack в Redis».

Поэтому:

- **Redis cursor** = «уже опубликовано в очередь» (батч + атомарно с Publish).
- **Ack store** = «уже обработано» (ack topic / `acked_at`).
- Recovery = Lock из ack store + Replay с cursor.

## Батчи (как в pgqueue Requester)

- `Publish` в PG — multi-row `INSERT`.
- Claim due — `FOR UPDATE SKIP LOCKED` + prefetch.
- Bridge SRQ: `ReadBatch` → один EVAL Publish+cursor; размер батча конфигурируемый.

## Emergency / resume stamp

Эвристики «это не transient»:

- явный admin `PauseJournalConsumers` / ключ `srq:resume_after=0` (пауза);
- Redis `LOADING`;
- опционально: детект пустого/урезанного `srq:queues` после рестарта при ненулевом ожидаемом cursor (осторожно, false positive).

Не триггерить на обычный `i/o timeout` с успешным ping следом.

## Rescheduler

- pg/mongo: due window `now()+10s` (конфиг).
- kafka-only far schedule: **не** в v1; dual driver `main+scheduled` (scheduled = pg) — backlog рядом с Connect driver.
- ordered `!` при reject с длинной задержкой: как в Requester — предпочтительно оставаться в Redis с block, не уносить в far store (иначе ломается порядок).

## Redpanda Connect (backlog)

Не HTTP ingest. Нативный output component в экосистеме Connect: batch → SRQ Publish+cursor Lua. Ack/recovery всё равно в Go-воркерах.

## Ключи (черновик)

| Ключ | Назначение |
|------|------------|
| `srq:journal:{name}:cursor` | opaque cursor string/int |
| `srq:resume_after` | unix ms; воркеры с `faultAt >= resume_after` ждут |
