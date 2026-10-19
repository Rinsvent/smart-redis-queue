# Debounce и идемпотентность

Похожие по цели, разные по моменту.

## Idempotency TTL после Ack

После успешного Ack можно оставить ключ `payload:{id}` на время TTL. Пока он жив, `Publish` с тем же `ID` не пройдёт (`SET NX`).

```go
consumer.SetIdempotencyTtl(2 * time.Second)
// или при ручном цикле: Ack(ctx, task.ID, 2*time.Second)
```

| | Idempotency TTL | Rate limit |
|--|-----------------|------------|
| Что режет | Повтор **того же** ID | Много разных событий за окно |
| Когда начинается | После успешного Ack | По времени / на попытку |
| Новый payload с тем же ID | Не принимается | Зависит от вашей политики |

Хватает, когда в `ID` стабильный бизнес-ключ (`invoice:2026-10-15:merchant_9`) и достаточно «успешный прогон этого ключа не чаще, чем раз в TTL».

Если в полёте нужно обновлять payload — смотрите debounce.

## Debounce (`deb!`)

Если `Task.ID` начинается с `deb!` (хелпер `DebounceID`), занятый id Publish не отклоняет:

| Состояние | Поведение |
|-----------|-----------|
| Задачи ещё нет | Обычный enqueue |
| Уже в очереди | Перезапись payload и сдвиг `Scheduled` |
| Уже in-flight | Перезапись payload; после Ack задача снова встаёт в очередь с актуальным телом |

```go
id := redisqueue.DebounceID("snapshot:project_7")
_ = producer.Publish(ctx, &redisqueue.Task{
    ID: id, Payload: latestJSON, Scheduled: time.Now().Add(time.Second),
})
```

Удобно для частых обновлений одного агрегата: реиндекс, пересчёт квот, «последний статус экспорта». В очереди остаётся свежий снимок, а не пачка устаревших задач.

Без префикса работает обычный `SET NX` и `ErrTasksAlreadyExist`.
