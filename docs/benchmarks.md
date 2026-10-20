# Бенчмарки

Цифры с текущего HEAD. Среда: Linux amd64, AMD Ryzen 7 2700, Redis на localhost:6379, `benchtime=1s`, `count=3`, в таблицах — медиана.

Сравнение коммитов и продовые регрессии лежат в репозитории (`docs/ANALYSIS.md`, `docs/bench-results/`) — это внутренний разбор, не часть пользовательской доки.

## Core (µs/op)

| Bench | HEAD |
|-------|-----:|
| Publish | 170 |
| PublishBatch100 | 2873 |
| Consume | 236 |
| Consume prefetch=10 | 204 |

Одиночный Publish/Consume остаётся в том же порядке, что и раньше. Batch Publish дороже из‑за индексов groups/ready — плата за фичи, не за «пустой» hot path.

## Redis CPU при парковке аккаунта

Сценарий: много партиций `!acct:*`, лимит на аккаунт.

| Сценарий | Результат |
|----------|-----------|
| Idle Get при `BlockGroups`, до 1000 партиций | ~80–95 µs Redis CPU, почти не растёт с N |
| IdleReject с `BlockGroups` vs reject только партиции | примерно **в 2.2 раза** меньше Redis CPU |

Правило для приложения: упёрлись в MAX аккаунта — паркуйте группу (`RejectWithDelay(..., accountGroup)`), а не каждую партицию ресурса.

## zstd

На хорошо сжимаемых телах от ~16–64 KiB:

- Publish часто быстрее (меньше данных в Redis);
- размер value падает на порядки на тестовом паттерне.

На ~1 KiB выигрыш по памяти есть, по latency — почти ноль.

## Шардинг

Параллельный Publish по группам / Consume пулом. Медиана, `benchtime=2s`. Все цифры — **один хост** (Redis + бенч на Ryzen 7 2700): упор в общий CPU, поэтому прирост скромный. На разных серверах ожидается больше. Подробности — [Шардинг](sharding.md#бенчмарки-локально).

| Сценарий | ns/op | vs baseline |
|----------|------:|------------:|
| Publish 1 DB | 56 000 | — |
| Publish 4 DB (один Redis) | 71 000 | без ×N Lua |
| Publish 4× redis-server (same host) | 37 000 | **~1.5×** (ceiling на 1 CPU) |
| Consume×8, 1 DB | 134 000 | — |
| Consume×8, 4 DB | 117 000 | **~1.15×** (same host) |

Multi-DB — меньше ключей на DB, небольшой выигрыш Consume. ×N Lua — отдельные процессы; на отдельных машинах — сильнее, чем ~1.5× с одной тачки.

```bash
go test -run='^$' -bench='BenchmarkShard' -benchtime=2s -count=3 -timeout=30m .

SRQ_SHARD_ADDRS=127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381,127.0.0.1:6382 \
  go test -run='^$' -bench='BenchmarkShardPublish_MultiProcess' -benchtime=2s -count=3 .
```

## Как прогнать у себя

```bash
# Redis на :6379
go test -run='^$' -bench='BenchmarkPublish$|BenchmarkConsume$' \
  -benchtime=1s -count=3 .

go test -run='^$' \
  -bench='BenchmarkPayload|BenchmarkCompress|BenchmarkBlocked|BenchmarkIdleReject' \
  -benchtime=1s -count=3 -timeout=45m .

go test -run='TestBlockedGroupGet_SlowlogBudget' -count=1 .
```
