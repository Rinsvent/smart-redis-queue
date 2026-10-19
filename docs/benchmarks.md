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
