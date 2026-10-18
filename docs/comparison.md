# Сравнение очередей

SRQ — не Kafka-replacement, а **Redis work-queue** с редкими first-class фичами (debounce, race!, dual RL, ordered reclaim).

Легенда: **Y** = из коробки; **P** = частично / своими руками; **—** = нет или не цель.

| Фича | Kafka | RabbitMQ | NATS core | NATS JetStream | **SRQ** |
|------|:-----:|:--------:|:---------:|:--------------:|:-------:|
| Persist / replay по offset | Y | P | — | Y | P (до Ack; journal — бэклог) |
| At-least-once + ack | Y | Y | — | Y | Y |
| Exactly-once end-to-end | P | — | — | P | **P** (см. ниже) |
| Порядок в partition/key | Y | P | — | Y | **Y** (`!` + locks) |
| Priority queues | — | Y | — | — | **Y** |
| Delayed / scheduled | P | Y | — | Y | **Y** |
| Consumer groups / competing | Y | Y | Y | Y | **Y** |
| Prefetch / batch get | Y | Y | — | Y | **Y** |
| Topic/exchange routing | Y | Y | Y | Y | P (groups/partitions) |
| Fan-out / pub-sub | Y | Y | Y | Y | — |
| Dead-letter / retry delay | Y | Y | — | Y | **Y** |
| Queue depth backpressure | P | Y | — | Y | **Y** (теги) |
| Token / sliding / leaky RL в брокере | — | P | — | P | **Y** (5 algo, group+partition) |
| Cross-partition mutex | — | — | — | — | **Y** (`race!`) |
| Debounce / latest payload | — | — | — | — | **Y** (`deb!`) |
| Idempotency window после success | P | — | — | P | **Y** |
| Payload compression | Y | Y | Y | Y | **Y** (zstd) |
| Horizontal scale | Y | Y | Y | Y | **Y\*** (= Redis) |
| Multi-tenant isolation | Y | Y | Y | Y | P (queue / DB / ACL) |
| UI / ops tooling | Y | Y | P | P | **Y** (CLI + Wails) |

\*Координация всегда через Redis; throughput = topology Redis + шардинг очередей. Не «scale без Redis».

## Уточнения

### Exactly-once

У Kafka EOS = транзакции брокера. У SRQ доставка **at-least-once** (Get → Ack; падение → reclaim).

Эффективный e2e once по *бизнесу*:

- тот же `Task.ID` + F1 idempotency TTL и/или идемпотентный handler;
- при необходимости ordered `!`.

Гарантии «брокер сам сделал EOS без идемпотентности handler» нет — как у большинства очередей. В матрице **P**, не «нет». Подробнее: [Гарантии доставки](delivery.md).

### Fan-out

Broadcast: один publish → много независимых подписчиков. SRQ = competing consumers → в матрице «—».

### Когда выбирать что

| Нужно | Выбор |
|-------|--------|
| Event log, replay, fan-out | Kafka / JetStream |
| Классический AMQP, enterprise routing | RabbitMQ |
| Уже есть Redis + порядок + API rate limits | **SRQ** |
| Только pub/sub без ack | NATS core / Redis PubSub |

Полный черновик анализа: [ANALYSIS.md](ANALYSIS.md).
