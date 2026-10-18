# Бенчмарки

Среда референсных прогонов: Linux amd64, AMD Ryzen 7 2700, Redis localhost:6379, `benchtime=1s`, `count=3`, в таблицах — **медиана**.

Сырьё: [`docs/bench-results/`](https://github.com/Rinsvent/smart-redis-queue/tree/main/docs/bench-results), нарратив — [ANALYSIS.md](ANALYSIS.md).

## Core (µs/op)

| Bench | prod₀ `a96f70e` | HEAD |
|-------|----------------:|-----:|
| Publish | 178 | 170 (−4%) |
| PublishBatch100 | 2091 | 2873 (+37%) |
| Consume | 220 | 236 (+7%) |
| Consume pf=10 | 196 | 204 (+4%) |

Интерпретация: одиночный Publish/Consume ≈ паритет с ранним prod; batch publish дороже из‑за индексов groups/ready (цена фич). Выигрыш F3–F6 — функциональный, не «ещё быстрее empty-queue microbench».

## Redis CPU / blocked account

Паттерн: много партиций `!acct:*`, лимит на аккаунт.

| Сценарий | Вывод |
|----------|--------|
| Idle Get при `BlockGroups` | ~80–95 µs Redis CPU, **flat vs N** (100…1000 parts) |
| IdleReject `block_group` vs `part_only` | ≈ **2.2×** меньше Redis CPU |
| Staging без group-block | Get ~77ms, Redis CPU у потолка |

**Правило для app:** упёрлись в MAX аккаунта → `RejectWithDelay(..., accountGroup)`, не reject каждой партиции ресурса.

## zstd

На хорошо сжимаемых ≥16–64 KiB:

- Publish часто **быстрее** (меньше сеть / memory bandwidth Redis);
- footprint value падает на **~100–2000×** на тестовом паттерне.

На ~1 KiB выигрыш по памяти есть, по latency — почти ноль.

## Как перезапустить

```bash
# Redis на :6379
./scripts/bench-commits.sh a96f70e 0959fb3 HEAD

BENCH_CPU=1 ./scripts/bench-commits.sh 0959fb3 HEAD

go test -run='^$' -bench='BenchmarkPublish$|BenchmarkConsume$' \
  -benchtime=1s -count=3 .

go test -run='TestBlockedGroupGet_SlowlogBudget' -count=1 .
```

Краткое summary: [`bench-results/SUMMARY.md`](bench-results/SUMMARY.md).
