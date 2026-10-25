# Journal — внутренние заметки

Публично: [`docs/features/journal.md`](../../docs/features/journal.md).

## Дыры курсора (не делать)

Time-курсор / ObjectID-from-scheduled; `offset > cursor AND scheduled <= now` с advance мимо undued; in-place ready без нового offset позади курсора.

## Scheduler

- pg/mongo: `state` + nullable `journal_offset`, batch `$inc`/`nextval`, `PromoteDue`.
- kafka: `ScheduleTopic` + `PromoteDue` → `InTopic`; cursor `srq:journal:{name}:kafka_sched`.

## Recovery

`RecoverAfterRedis` → `ReadAcks` + `Producer.Lock` (kafka) → pump. pg/mongo режут по `acked_at`.
