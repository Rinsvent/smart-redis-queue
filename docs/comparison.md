# Сравнение очередей

SRQ — очередь задач на Redis. Не пытается заменить Kafka как лог событий; зато закрывает порядок, delayed, dual rate limit, debounce и `race!` без второго брокера.

Легенда: **Y** — есть из коробки; **P** — частично или своими руками; **—** — нет или не цель.

| Фича | Kafka | RabbitMQ | NATS core | NATS JetStream | **SRQ** |
|------|:-----:|:--------:|:---------:|:--------------:|:-------:|
| Persist / replay по offset | Y | P | — | Y | P (живёт до Ack; журнал — в планах) |
| At-least-once + ack | Y | Y | — | Y | Y |
| Exactly-once end-to-end | P | — | — | P | **P** (см. ниже) |
| Порядок в partition/key | Y | P | — | Y | **Y** (`!`) |
| Priority queues | — | Y | — | — | **Y** |
| Delayed / scheduled | P | Y | — | Y | **Y** |
| Competing consumers | Y | Y | Y | Y | **Y** |
| Prefetch / batch get | Y | Y | — | Y | **Y** |
| Topic/exchange routing | Y | Y | Y | Y | P (groups / partitions) |
| Fan-out / pub-sub | Y | Y | Y | Y | — |
| Retry delay / dead-letter | Y | Y | — | Y | **Y** |
| Backpressure по глубине | P | Y | — | Y | **Y** (теги) |
| Token / sliding / leaky RL в брокере | — | P | — | P | **Y** (группа + партиция) |
| Mutex между партициями | — | — | — | — | **Y** (`race!`) |
| Debounce / latest payload | — | — | — | — | **Y** (`deb!`) |
| Окно идемпотентности после успеха | P | — | — | P | **Y** |
| Сжатие payload | Y | Y | Y | Y | **Y** (zstd) |
| Горизонтальный масштаб | Y | Y | Y | Y | **Y\*** (через Redis) |
| Multi-tenant isolation | Y | Y | Y | Y | P (queue / DB / ACL) |
| UI / ops | Y | Y | P | P | **Y** (CLI + Desktop) |

\*Координация всегда через Redis. Throughput растёт вместе с topology Redis и шардингом очередей — не «масштаб без Redis».

## Exactly-once

У Kafka EOS завязан на транзакции брокера. У SRQ доставка **at-least-once**: Get → Ack, при падении — reclaim.

«Ровно один раз» по бизнесу обычно собирают так:

- стабильный `Task.ID` и идемпотентный handler;
- при желании — idempotency TTL после Ack (повторный Publish того же ID не примется, пока жив TTL);
- ordered `!`, если важен порядок в ключе.

Брокер сам не обещает EOS без идемпотентности на вашей стороне — как у большинства очередей. Подробнее: [Гарантии доставки](delivery.md).

## Fan-out

Fan-out — это broadcast: одно сообщение получают несколько независимых подписчиков. SRQ так не работает: задачу забирает один воркер.

## Что выбрать

| Нужно | Обычно берут |
|-------|----------------|
| Event log, replay, fan-out | Kafka / JetStream |
| Классический AMQP и сложный routing | RabbitMQ |
| Уже есть Redis + порядок + лимиты API | **SRQ** |
| Лёгкий pub/sub без ack | NATS core / Redis PubSub |
