# Journal — внутренние заметки

Публично: [`docs/features/journal.md`](../../docs/features/journal.md).

## Дыры курсора (не делать)

1. Time-курсор / ObjectID «от scheduled» + late insert → skip.
2. `ReadBatch: offset > cursor AND scheduled <= now` при advance cursor мимо undued → skip навсегда.
3. In-place `state=ready` **без нового** journal_offset, если cursor уже прошёл «дырявый» id → skip.

Правило: в ready-stream попадает только строка с **новым** journal_offset в момент готовности.

## pg/mongo scheduler

Одна таблица: `scheduled` без offset → `PromoteDue` выдаёт batch-reserve offset, payload на месте.  
Mongo counter: `$inc: {seq: n}` один раз на батч.  
Kafka: отдельный журнал + mover (ref/dual store).

## Publish

`PublishWith(WithJournal)` only. Шардинг: один шард на journal-батч.
