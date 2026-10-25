# Journal — внутренние заметки

Публично: [`docs/features/journal.md`](../../docs/features/journal.md).

## Offset вместо time-курсора

Time-курсор `(scheduled, task_id)` ломается при двух продюсерах / долгом prepare: «поздний» батч сдвигает курсор → опоздавшее сообщение skip. Unified model = **append offset** (как Kafka). Порядок между независимыми продюсерами не обещаем.

Far-future: не дырявить main offset-stream; scheduled store → insert в main с **новым** offset когда due.

## Redis pump lock

`srq:journal:{name}:pump` NX TTL 60s, refresh 10s — единый single-writer для pg/mongo/kafka. Источник-specific locks optional.

## Requester

[Rinsvent/requester](https://github.com/Rinsvent/requester): OutRelock, OffsetRef+Fetcher, PG mover. Публично не ссылаем.

## Инфра

`docker-compose.yaml`: redis, postgres, mongo, kafka. Тесты драйверов skip без сервиса.
