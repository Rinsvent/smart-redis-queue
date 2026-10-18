# Bench results summary

Host: Ryzen 7 2700, Redis localhost:6379, go1.26.2, `benchtime=1s`, `count=3`, median.

## Cross-commit core (µs/op)

| Bench | a96f70e | 0959fb3 | 787467d | 83fd113 | 9250ef6 | ed29547 (HEAD) |
|-------|--------:|--------:|--------:|--------:|--------:|---------------:|
| Publish | 178 | 171 | 167 | 174 | 171 | 170 |
| PublishBatch100 | 2091 | 2810 | 2508 | 2469 | 2526 | 2873 |
| Consume | 220 | 218 | 233 | 218 | 230 | 236 |
| Consume pf=10 | 196 | 209 | 213 | 259 | 199 | 204 |

Δ HEAD vs a96f70e: Publish −4%, Batch100 +37%, Consume +7%, pf10 +4%.

## Redis CPU / blocked connector (`*-cpu.txt`)

Медиана `redis_cpu_us/op`. Сырьё: `0959fb3-cpu.txt`, `787467d-cpu.txt`, `244f70d-cpu.txt`.

| Bench | 0959fb3 | 787467d | HEAD |
|-------|--------:|--------:|-----:|
| BlockedGroup idle / parts-1000 | 82 | 81 | 92 |
| IdleReject part_only / parts-500 | 222 | 222 | 223 |
| IdleReject block_group / parts-500 | 105 | 88 | 92 |
| ParkedPartitions idle / parts-500 | 498 | 553 | 538 |

`block_group` ≈ 2.2× дешевле `part_only` по Redis CPU; BlockedGroup idle ≈ flat vs N.

Slowlog guard: `go test -run=TestBlockedGroupGet_SlowlogBudget`.

Raw core: `a96f70e.txt`, …  
Scenarios: `HEAD.txt` / `ed29547.txt`.  
Narrative: [`../ANALYSIS.md`](../ANALYSIS.md) §1.3.
