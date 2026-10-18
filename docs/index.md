# Smart Redis Queue

Очередь задач на **Redis** с партициями, приоритетами, отложенным выполнением, строгим порядком и first-class контролем скорости — без отдельного брокера Kafka/RabbitMQ.

[![CI](https://github.com/Rinsvent/smart-redis-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Rinsvent/smart-redis-queue/actions/workflows/ci.yml)
[![GitHub](https://img.shields.io/badge/GitHub-Rinsvent%2Fsmart--redis--queue-blue?logo=github)](https://github.com/Rinsvent/smart-redis-queue)

## Что это

**Smart Redis Queue (SRQ)** — Go-библиотека work-queue поверх Redis:

- competing consumers (одну задачу берёт один воркер);
- ordered-партиции (`!`) с reclaim без ломания порядка;
- группы + dual rate limit на Get (квота аккаунта ∧ квота ресурса);
- debounce (`deb!`), concurrency tags (`race!`), tag backpressure, zstd;
- Admin / CLI / Desktop / Mobile / Web UI для ops.

Это **не** замена Kafka как event log и **не** pub/sub fan-out. Это очередь работ там, где у вас уже есть Redis и нужны порядок + лимиты API.

## Когда подходит

| Подходит | Скорее нет |
|----------|------------|
| Воркеры ходят во внешние API с rate limit | Нужен Kafka-like replay по offset |
| Нужен порядок по ключу (заказ, пользователь, ресурс) | Нужен broadcast одному сообщению → N подписчиков |
| Redis уже в стеке, не хочется ещё один брокер | Масштаб «без shared Redis» |
| Ops: purge / block / retry / графики | SaaS multi-tenant из коробки |

## Быстрые ссылки

- [Зачем SRQ, а не Kafka / Rabbit](why.md)
- [Быстрый старт](getting-started.md)
- [Фичи](features/index.md)
- [Сравнение](comparison.md)
- [Приложения и сервер](apps.md)
- [GitHub](https://github.com/Rinsvent/smart-redis-queue)

## Установка

```bash
go get github.com/Rinsvent/smart-redis-queue
```

Требования: **Go 1.26+** (см. `go.mod`), **Redis 6+**.
