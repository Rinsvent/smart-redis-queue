# Зачем Smart Redis Queue

## Проблема

Многие сервисы уже живут на Redis: кэш, сессии, locks, rate-limiter. Когда появляются фоновые задачи с **порядком** и **квотами внешнего API**, типичный путь — поднять Kafka или RabbitMQ.

Это работает, но часто избыточно:

- ещё один кластер, мониторинг, ACL, релизы;
- rate limit и «последний payload» всё равно пишутся в приложении;
- Redis уже есть — координация дублируется.

SRQ остаётся **work-queue на Redis**: те же гарантии ack/reclaim, но фичи, ради которых обычно пишут middleware вокруг брокера, — first-class.

## Почему не «просто Redis Lists / Streams»

| Подход | Чего не хватает из коробки |
|--------|----------------------------|
| `LPUSH` / `BRPOP` | Порядок по ключу, priority, delayed, dead-consumer reclaim |
| Redis Streams | Dual RL group∧partition, debounce latest-wins, cross-partition mutex |
| Свой Lua ad-hoc | Ops UI, стабильный API, регрессионные бенчи |

SRQ — готовая модель ключей + Lua + Go API + Admin/UI.

## Почему не Kafka

Kafka сильна как **лог событий**: replay по offset, fan-out, scale брокеров.

SRQ — другая задача:

| | Kafka | SRQ |
|--|:-----:|:---:|
| Модель | Append log | Work queue (Get → Ack) |
| Replay | First-class | Бэклог (journal) |
| Fan-out | Да | Нет (одна задача → один воркер) |
| Priority / delayed | Слабо / своими руками | Да |
| Broker-side token/sliding RL | Нет | Да (5 алгоритмов) |
| Debounce latest payload | Нет | `deb!` |
| Cross-partition mutex | Нет | `race!` |
| Scale | Брокеры Kafka | Topology Redis |

Итог: **не замена Kafka**, а очередь работ рядом с Redis. Если нужен event bus и аудит-лог на годы — Kafka/Stream остаётся своим классом.

## Почему не RabbitMQ

Rabbit закрывает durable queues, priority, delayed (плагины), prefetch.

Чего обычно нет first-class:

- dual rate limit на стороне выдачи (квота аккаунта **и** квота ресурса в одном Get);
- debounce «всегда последний payload»;
- mutex ресурса между партициями (`race!`);
- окно идемпотентности после успешного Ack.

При этом вы тащите AMQP-кластер, хотя Redis уже обслуживает похожие паттерны.

## Отказ от сложных инструментов, когда есть Redis

Практичное правило:

1. **Redis уже в проде** → сначала очередь на нём.
2. Нужны **порядок + RL + debounce** → SRQ закрывает это без второго брокера.
3. Нужен **долгоживущий log / multi-subscriber replay** → отдельный брокер/хранилище, не SRQ.

Масштаб SRQ = масштаб Redis (multi-DB, несколько инстансов, Cluster при аккуратном key design). Формулировка «Kafka масштабируется без shared Redis» — про другую архитектуру, не минус work-queue на Redis.

## Типичный кейс (без «чатов»)

Воркеры вызывают внешний HTTP API с лимитами:

- **на аккаунт / API-ключ** — не больше N запросов/сек;
- **на ресурс** (заказ, документ, mailbox) — строгий порядок и свой меньший лимит.

Партиция = `!{account}:{resource}`, группа = `acct:{account}`. При упирании в MAX аккаунта паркуется **группа**, а не тысячи отдельных ресурсов. Get под запаркованной группой остаётся дешёвым (~O(1) по числу партиций).

Подробнее: [Группы и rate limit](features/rate-limit.md), [Сравнение](comparison.md).
