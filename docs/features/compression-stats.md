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

Несколько хостов пишут в одни и те же секундные ключи — счётчики суммируются. Чтобы видеть «всего в очереди», тегируйте задачи (например `all`) и добавьте этот тег в `SetTagsStats`.

В Desktop UI есть график get/ack/reject и локальный watch тегов (poll на машине, в Redis ничего не пишет).
