# Smart Redis Queue

Очередь задач на **Redis**: партиции, приоритеты, отложенный запуск, строгий порядок и rate limit — без отдельного брокера.

[![CI](https://github.com/Rinsvent/smart-redis-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Rinsvent/smart-redis-queue/actions/workflows/ci.yml)
[![GitHub](https://img.shields.io/badge/GitHub-Rinsvent%2Fsmart--redis--queue-blue?logo=github)](https://github.com/Rinsvent/smart-redis-queue)

## Коротко

**Smart Redis Queue (SRQ)** — Go-библиотека поверх Redis. Несколько воркеров берут задачи из одной очереди; каждую задачу обрабатывает один из них.

Из коробки:

- ordered-партиции (`!`) — порядок и reclaim после падения воркера;
- группы и rate limit на Get (квота аккаунта и квота ресурса);
- debounce (`deb!`), concurrency tags (`race!`), backpressure по тегам, сжатие zstd;
- Admin, CLI, Desktop, Mobile и Web UI для обслуживания.

Это не event log вроде Kafka и не pub/sub. Обычная очередь задач: уже есть Redis, нужны порядок и лимиты внешнего API.

## Когда имеет смысл

| Подходит | Лучше другой инструмент |
|----------|-------------------------|
| Воркеры бьют во внешние API с rate limit | Нужен replay по offset, как в Kafka |
| Нужен порядок по ключу (заказ, пользователь, документ) | Одно сообщение должно уйти сразу многим подписчикам |
| Redis уже в стеке, второй брокер не хочется | Масштаб «без общего Redis» |
| Нужны purge / block / retry и простой UI | Готовый SaaS multi-tenant |

## Куда дальше

- [Зачем SRQ, а не Kafka / Rabbit](why.md)
- [Быстрый старт](getting-started.md)
- [Фичи](features/index.md)
- [Сравнение](comparison.md)
- [Приложения и сервер](apps.md)
- [Репозиторий](https://github.com/Rinsvent/smart-redis-queue)

## Установка

```bash
go get github.com/Rinsvent/smart-redis-queue
```

Нужны **Go 1.26+** (см. `go.mod`) и **Redis 6+**.
