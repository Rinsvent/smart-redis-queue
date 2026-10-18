# Debounce и идемпотентность

Две разные вещи. Обе про «не делать лишнюю работу», но в разные моменты жизненного цикла.

## Idempotency TTL после Ack

После успешного Ack при `idempotencyTtl > 0` ключ `payload:{id}` не удаляется, а ставится маркер + `EXPIRE`. Пока TTL жив, `Publish` с тем же `ID` не проходит (`SET NX`).

```go
consumer.SetIdempotencyTtl(2 * time.Second)
// или Ack(ctx, task.ID, 2*time.Second) при ручном цикле
```

| | Idempotency TTL | Rate limit |
|--|-----------------|------------|
| Что ограничивает | Повтор **того же** ID | N разных событий за окно |
| Когда стартует | После **успешного** Ack | На attempt / по времени |
| Новый payload с тем же ID | Не принимается | Зависит от политики |

**Когда хватает:** стабильный бизнес-ключ в `ID` (`invoice:2026-10-15:merchant_9`) + при необходимости ordered-партиция.

**Когда не хватает:** нужны обновления payload в полёте → debounce.

## Debounce Publish (`deb!`)

Если `Task.ID` начинается с `deb!` (хелпер `DebounceID`), Publish **не** отклоняется при занятом id:

| Состояние | Поведение |
|-----------|-----------|
| Нет задачи | Обычный enqueue |
| В очереди | Перезапись payload + сдвиг `Scheduled` |
| In-flight | Перезапись payload + флаг; после **Ack** задача снова в ZSET с актуальным payload |

```go
id := redisqueue.DebounceID("snapshot:project_7")
_ = producer.Publish(ctx, &redisqueue.Task{
    ID: id, Payload: latestJSON, Scheduled: time.Now().Add(time.Second),
})
```

Кейс: частые обновления одного агрегата (реиндекс, пересчёт квот, «последний статус экспорта») — в очереди всегда актуальный снимок, без шторма устаревших задач.

Без префикса — прежний `SET NX` / `ErrTasksAlreadyExist`.
