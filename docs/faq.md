# FAQ

## SRQ заменяет Kafka?

Нет. Kafka — лог событий, replay и fan-out. SRQ — очередь задач на Redis с порядком и rate limit. См. [Зачем SRQ](why.md) и [Сравнение](comparison.md).

## Нужен ли отдельный Redis только под очередь?

Не обязательно. Часто используют тот же инстанс, что и для кэша. При высокой нагрузке очереди лучше вынести на отдельный Redis или хотя бы на другую DB.

## At-least-once значит возможны дубли?

Да. Делайте handler идемпотентным и/или включите [idempotency TTL](features/debounce-idempotency.md).

## Чем rate limit группы отличается от backpressure по тегу?

| | Rate limit / BlockGroups | Tag backpressure |
|--|--------------------------|------------------|
| Когда | При выдаче или после 429 | При Publish |
| Что режет | Скорость обработки | Глубину очереди |
| Scope | group / partition | tag |

## Можно ли несколько групп на задачу?

Да, но для типового лимита API хватает одной группы на аккаунт. Несколько групп усложняют обход ready.

## Desktop и server — один бинарь?

Нет. Desktop собирается с `-tags wails` (на Linux ещё GTK). Server — с `-tags server`, без CGO. См. [Приложения](apps.md).
