# Journal — внутренние заметки

Публично: [`docs/features/journal.md`](../../docs/features/journal.md).

## Offset вместо time-курсора

Time-курсор `(scheduled, task_id)` ломается при двух продюсерах / долгом prepare. Unified model = append offset. Порядок между независимыми продюсерами не обещаем.

`scheduled`: `journal.ScheduledOrNow` — zero→now, иначе as-is (pg/mongo/mem одинаково).

Far-future / scheduled-mover: отдельный store → insert в main с новым offset когда due — **не реализован**.

Mongo: offset = ObjectID (`Cursor.Key`), InsertMany без counter collection.

## Publish

Только `PublishWith(WithJournal(...))` → `publishLocal` / Lua `jn`. Отдельного PublishJournalBatch нет.

Шардинг: батч с journal на >1 шард → ошибка. Bridge.rdb = Redis очереди (и cursor, и pump).

## Redis pump lock

`srq:journal:{name}:pump` NX TTL 60s, refresh 10s.

## Requester

[Rinsvent/requester](https://github.com/Rinsvent/requester): OutRelock, OffsetRef+Fetcher, PG mover. Публично не ссылаем.

## Инфра

`docker-compose.yaml`: redis, postgres, mongo, kafka. Тесты драйверов skip без сервиса.
