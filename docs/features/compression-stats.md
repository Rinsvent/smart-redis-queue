# Сжатие и метрики

## zstd на Producer

Крупный JSON и текст занимают память Redis и раздувают аргументы Lua. Можно включить сжатие (порог по умолчанию — 1 KiB):

```go
producer.SetCompression(redisqueue.CompressionZstd)
// или точечно: PublishWith(..., WithCompression(CompressionZstd), ...)
```

В Redis лежит маркер `RQC` и zstd; Get и Admin отдают уже разжатый `Task.Payload`. Старые несжатые значения читаются как раньше. Без `SetCompression` на сжатие CPU не тратится.

На хорошо сжимаемых телах от ~16 KiB Publish часто становится быстрее, а размер значения в Redis падает сильно. Уже сжатые бинарники (jpeg, gzip) лучше померить отдельно — там zstd может только добавить CPU. См. [Бенчмарки](../benchmarks.md).

## Метрики

По умолчанию выключены.

```go
consumer.SetStatsRetention(time.Hour)          // get/ack/reject раз в секунду
consumer.SetTagsStats("all", "campaign:spring") // SCARD выбранных тегов ~раз в секунду
```

Несколько хостов пишут в одни и те же секундные ключи — счётчики суммируются.

**Total очереди** (ready + in-progress) ведётся в `srq:queues` автоматически (Publish/Ack). В Desktop UI линия **total** на графике Queue rates — live-семпл этого score; если реестра ещё нет (старый Redis без producer/consumer после обновления) — линия не рисуется. Тег `all` для размера очереди больше не нужен.

Для срезов по бизнесу по-прежнему удобны теги + `SetTagsStats` / watch в UI.

В Desktop UI: get/ack/reject (нужен `SetStatsRetention` на воркере), **total** из `srq:queues`, локальный watch тегов (poll на машине, в Redis не пишет).
