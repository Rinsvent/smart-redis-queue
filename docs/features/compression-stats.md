# Сжатие и метрики

## zstd на Producer (opt-in)

Большие JSON/текст жрут память Redis и раздувают EVAL ARGV. Порог по умолчанию 1 KiB:

```go
producer.SetCompression(redisqueue.CompressionZstd)
// или PublishWith(..., WithCompression(CompressionZstd), ...)
```

В Redis — маркер `RQC` + zstd; Get/Admin отдают уже разжатый `Task.Payload`. Старые raw-значения работают. Без `SetCompression` — ноль CPU на сжатие.

На хорошо сжимаемых ≥16–64 KiB zstd часто **ускоряет** Publish и режет footprint на порядки. Плохо сжимаемые бинарники (jpeg/gzip) меряйте отдельно. См. [Бенчмарки](../benchmarks.md).

## Метрики очереди (opt-in)

По умолчанию выключены.

```go
consumer.SetStatsRetention(time.Hour)          // g/a/r раз в секунду
consumer.SetTagsStats("all", "campaign:spring") // SCARD тегов ~1s
```

- **get / ack / reject** — flush в `queue:{q}:stats:{sec}`; несколько хостов суммируются
- **TagsStats** — только указанные теги → поля `t:{tag}`
- Чтобы видеть «total», тегируйте задачи (например `all`) и добавьте тег в `SetTagsStats`

Desktop UI: график get/ack/reject + watch тегов (локальный poll, без записи в Redis).
