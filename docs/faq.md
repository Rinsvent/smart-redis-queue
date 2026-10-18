# FAQ

## SRQ заменяет Kafka?

Нет. Kafka — event log / replay / fan-out. SRQ — work-queue на Redis с порядком и rate limit. См. [Зачем SRQ](why.md) и [Сравнение](comparison.md).

## Нужен ли отдельный Redis только под очередь?

Нет. Часто используют тот же инстанс, что и кэш. При высокой нагрузке выносите очереди на отдельный Redis или другую DB.

## At-least-once значит возможны дубли?

Да. Делайте handler идемпотентным и/или используйте [idempotency TTL](features/debounce-idempotency.md).

## Чем group rate limit отличается от backpressure по тегу?

| | Get RateLimit / BlockGroups | Tag backpressure |
|--|----------------------------|------------------|
| Когда | При выдаче / после 429 | При Publish |
| Что режет | Скорость обработки | Глубину очереди |
| Scope | group / partition | tag SCARD |

## Можно ли несколько групп на задачу?

Да, но для типового API-лимита достаточно одной группы на аккаунт. Несколько групп усложняют обход ready.

## Desktop и server — один бинарь?

Нет. Desktop: `-tags wails` (+ GTK на Linux). Server: `-tags server`, без CGO. См. [Приложения](apps.md).

## Где смотреть сырые бенчи?

[`docs/bench-results/`](https://github.com/Rinsvent/smart-redis-queue/tree/main/docs/bench-results) и [ANALYSIS.md](ANALYSIS.md).
