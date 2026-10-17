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

Raw: `a96f70e.txt`, `0959fb3.txt`, …, `ed29547.txt`.  
Scenarios (payload/zstd/ordered): `HEAD.txt` / `ed29547.txt`.  
Narrative: [`../ANALYSIS.md`](../ANALYSIS.md).
