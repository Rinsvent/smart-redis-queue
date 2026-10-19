# Smart Redis Queue — анализ производительности и позиционирования

Внутренний разбор (сравнение коммитов, prod-регрессии). На сайт GitHub Pages не публикуется — там только цифры с HEAD, см. `benchmarks.md`.  
Сырые прогоны: [`docs/bench-results/`](bench-results/).

**Среда прогонов (этот документ):** Linux amd64, AMD Ryzen 7 2700, Redis localhost:6379, Go из `go env GOVERSION`, `benchtime=1s`, `count=3`, в таблицах — **медиана** ns/op.

---

## 1. Сравнение коммитов (эволюция перфоманса)

### 1.1 Ключевые точки

| SHA | Дата | Что это | Зачем в выборке |
|-----|------|---------|-----------------|
| `a96f70e` | 2026-07-04 | `feat: fix locks and add check option` | **Prod baseline** — то, с чего начинали на проде |
| `0959fb3` | 2026-10-05 | random iter + group block | **Поздний prod** — обновление после baseline |
| `787467d` | после 0959fb3 | script optimization | Оптимизации Lua / обхода |
| `83fd113` | позже | `:count` для non-ordered | Меньше Redis-работы на unordered |
| `9250ef6` | позже | ping optimization | Дешёвый heartbeat |
| `HEAD` (`ed29547`+) | 2026-10 | backpressure, RL, debounce, race!, compress, collectOne refresh | Текущее состояние после фич и оптимизаций |

Прогон: `./scripts/bench-commits.sh` (worktree на каждый SHA).  
Общий набор для всех коммитов:

```
BenchmarkPublish$|BenchmarkPublishBatch100|BenchmarkConsume$|BenchmarkConsumePrefetch/prefetch-10
```

На `HEAD` дополнительно: `scenarios_bench_test.go` (payload / zstd / ordered) и `scenarios_cpu_bench_test.go` (blocked group / idle reject / Redis CPU).

CPU-only matrix (не затирает core `.txt`):

```bash
BENCH_CPU=1 ./scripts/bench-commits.sh 0959fb3 787467d HEAD
# → docs/bench-results/<sha>-cpu.txt
```

### 1.2 Результаты core-бенчей (медиана µs/op)

Прогон: `docs/bench-results/{a96f70e,0959fb3,787467d,83fd113,9250ef6,ed29547}.txt`  
(один host, Redis :6379, `count=3`, медиана). В скобках — Δ к **a96f70e**.

| Benchmark | a96f70e (prod₀) | 0959fb3 (prod₁) | 787467d script opt | 83fd113 `:count` | 9250ef6 ping | HEAD `ed29547` |
|-----------|----------------:|----------------:|-------------------:|-----------------:|-------------:|---------------:|
| Publish | 178 | 171 (−4%) | 167 (−6%) | 174 (−2%) | 171 (−4%) | **170 (−4%)** |
| PublishBatch100 | 2091 | 2810 (+34%) | 2508 (+20%) | 2469 (+18%) | 2526 (+21%) | **2873 (+37%)** |
| Consume (pf=1) | 220 | 218 (−1%) | 233 (+6%) | 218 (−1%) | 230 (+5%) | **236 (+7%)** |
| Consume pf=10 | 196 | 209 (+7%) | 213 (+9%) | 259 (+32%) | 199 (+2%) | **204 (+4%)** |

Шум: повторный прогон HEAD (`HEAD.txt`) дал Consume ~284 µs при том же коде — разброс RTT/GC порядка ±20–30%. Смотреть тренды, не десятые процента.

**Интерпретация:**

1. **Single Publish** к baseline не деградировал (даже чуть лучше). Hot path add без batch остаётся дешёвым.
2. **PublishBatch100** заметно хуже с `0959fb3` (+34%) и на HEAD (+37%): цена индексов groups/ready/blocked на каждую задачу в батче. Script/` :count` частично отыграли (до ~+18%), фичи F3–F6 снова подняли batch cost.
3. **Consume pf=1** в пределах шума (+7% на HEAD). Оптимизации `:count`/ping не дают явного выигрыша на *этом* микробенче (unordered, без groups/RL).
4. **Prefetch-10** всплеск на `83fd113` (+32%) потом сошёл; HEAD ≈ +4% к baseline.
5. **Вывод «стало ли лучше после оптимизаций»:** относительно **prod₀** одиночный Publish/Consume — паритет; batch publish — **хуже**. Относительно **prod₁** (`0959fb3`) batch почти такой же, single path не хуже. Выигрыш последних фич — функциональный (RL/debounce/race/compress), не latency empty-queue microbench. Где реально мерять рост — §3 (ordered, payload, zstd) и **§1.4 (Redis CPU / blocked connector)**.

### 1.3 Redis CPU / blocked account (медиана)

Prod-кейс: партиция = `!<account>:<resource>`, лимит на аккаунт; без group-block — до N холостых `Get → Reject` и Get O(партиций) по CPU Redis.

Метрики из `INFO CPU` / `COMMANDSTATS` / `SLOWLOG` (см. `scenarios_cpu_bench_test.go`):

| Метрика | Смысл |
|---------|--------|
| `redis_cpu_us/op` | Δ(used_cpu_sys+user) Redis / op, µs |
| `eval_us/op` | usec eval+evalsha / op |
| `slow_max_us` / `slow_n` | max и count SLOWLOG (порог записи 5ms на время бенча) |
| `deliver_frac` | доля Get, вернувших задачу (IdleReject) |

Сырьё: `docs/bench-results/{0959fb3,787467d,244f70d}-cpu.txt` (один host, `count=3`, медианы).

#### BlockedGroup idle Get (группа уже запаркована)

| parts | 0959fb3 redis_cpu | 787467d | HEAD |
|------:|------------------:|--------:|-----:|
| 100 | 80 | 85 | 92 |
| 500 | 80 | 85 | 94 |
| 1000 | 82 | 81 | 92 |

**Вывод:** с момента появления `BlockGroups` (`0959fb3`) idle Get под запаркованной группой **не растёт с N** (~80–95 µs Redis CPU). Это как раз фикс «Get 77ms / pod CPU 490m» при массовой парковке коннектора. Дальнейшие коммиты не ухудшили.

#### IdleReject connector (Get → Reject), parts=500

| Режим | 0959fb3 cpu | HEAD cpu | deliver_frac (HEAD) |
|-------|------------:|---------:|--------------------:|
| `part_only` (Reject только чат) | 222 | **223** | ~0.03 |
| `block_group` (Reject + BlockGroups) | 105 | **92** | ~0.00015 |

**Вывод:** `block_group` ≈ **2.2–2.4× меньше** Redis CPU на попытку Get и почти ноль холостых выдач. Паттерн для app: после упирания в MAX на аккаунт — `RejectWithDelay(..., accountGroup)`, не только partition.

#### Parked partitions idle Get (все чаты в blocked, группа свободна)

| parts | 0959fb3 cpu | HEAD cpu |
|------:|------------:|---------:|
| 100 | 182 | 214 |
| 500 | 498 | 538 |

Здесь CPU **растёт с N** (обход ready/admission). Не деградация HEAD vs 0959fb3 (шум/фичи), а оставшийся O(parked)-класс; group-block его обходит.

#### Slowlog-алерт в тестах

```bash
go test -run='TestBlockedGroupGet_SlowlogBudget' -count=1 .
# жёстче (5ms): SRQ_BENCH_STRICT=1 go test -run='TestBlockedGroupGet_SlowlogBudget' .
```

На HEAD при 1000 parts + BlockGroups slowlog пуст (`slow_max=0`, Lua ≪ 5ms). Тест падает, если Lua Get снова уйдёт в десятки ms. Это не замена prod-мониторинга SLOWLOG, а регрессионный предохранитель в CI/local.

#### Partition-scan load на `a96f70e` (отчёт S1000)

Воспроизведение: 1000 `!conn:*`, 4 воркера, 15 Ack/с, остальное `Reject(2s)` → `:block` на чат (без group-block).  
`BENCH_SCAN=1 ./scripts/bench-commits.sh a96f70e` → `docs/bench-results/a96f70e-scan.txt`.

| | Staging S1000 (отчёт) | Local `a96f70e` 20s | Local HEAD (тот же тест, без BlockGroups) |
|--|----------------------:|--------------------:|------------------------------------------:|
| Redis CPU | ≈450m (лимит пода 500m) | **980m** | 977m |
| EVALSHA/s | ≈1690 | **1315** | 1834 |
| avg EVAL µs | ≈240 | **705** | 493 |
| Lua probes/s | GET ≈154k | **EXISTS+GET ≈555k** | GET ≈2.9k (EXISTS=0) |
| SMEMBERS/s | ≈570 | **884** | 1123 |
| Reject/s | ≈560 | **416** | 438 |
| Ack/s | ≈15 | **15.1** | 11.6 |
| slowlog max | Get 77ms | 4–9ms | ≈3ms |

**Вывод:** на `a96f70e` паттерн отчёта ловится: Redis CPU у потолка, сотни Reject/с при 15 Ack/с, **сотни тысяч EXISTS/s** на `:block` (в отчёте это было GET — тот же класс O(партиций) probe).  
`SRQ_EXPECT_SCAN_COST=1` на этом коммите — жёсткий assert (avg EVAL≥80µs, CPU≥100m, probes≥2k/s).  
На HEAD без `BlockGroups` probes резко падают (ZSET blocked), но CPU от IdleReject всё ещё высокий — нужен group-block в app (`Reject(..., connector)`), см. IdleReject выше.

### 1.4 Как перезапустить

```bash
# Redis на :6379
./scripts/bench-commits.sh a96f70e 0959fb3 787467d 83fd113 9250ef6 HEAD
# CPU / blocked connector (отдельные *-cpu.txt):
BENCH_CPU=1 ./scripts/bench-commits.sh 0959fb3 787467d HEAD
# только сценарии на текущем дереве:
go test -run='^$' -bench='BenchmarkPayload|BenchmarkCompress|BenchmarkOrdered|BenchmarkBlocked|BenchmarkParked|BenchmarkIdleReject' \
  -benchtime=1s -count=3 -timeout=45m .
go test -run='TestBlockedGroupGet_SlowlogBudget' -count=1 .
```

---

## 2. Классификация фич брокеров и матрица сравнения

### 2.1 Классы возможностей

| Класс | Смысл | Типичные реализации |
|-------|--------|---------------------|
| **Transport / durability** | Доставка, персист, ack, at-least-once | Kafka log, Rabbit durable queues, NATS JetStream, Redis keys |
| **Ordering** | Порядок в ключе/партиции | Kafka partition, Rabbit single-active / quorum, NATS subject+consumer, SRQ `!` |
| **Routing** | Куда класть / кого будить | Kafka topics, Rabbit exchanges, NATS subjects, SRQ partitions+groups |
| **Backpressure** | Ограничение глубины/скорости publish | Kafka quotas, Rabbit max-length, NATS limits, SRQ TagBackpressure |
| **Rate / concurrency control** | Не чаще N / не больше K параллельно | app-side; Rabbit prefetch; SRQ Get-RL + `race!` |
| **Dedup / latest-wins** | Идемпотентность или debounce | Kafka idempotent producer; SRQ F1 TTL + `deb!` |
| **Payload efficiency** | Сжатие / сериализация | Kafka compression, Rabbit, NATS; SRQ zstd на payload |
| **Ops** | UI, purge, inspect, DLQ | экосистема брокера; SRQ Admin + Wails UI |

### 2.2 Матрица: Kafka / RabbitMQ / NATS / JetStream / SRQ

Легенда: **Y** = из коробки / first-class; **P** = частично / своими руками; **—** = нет или не цель продукта.

| Фича | Kafka | RabbitMQ | NATS core | NATS JetStream | **SRQ** |
|------|:-----:|:--------:|:---------:|:--------------:|:-------:|
| Persist / replay по offset | Y | P (quorum/stream) | — | Y | P (до Ack в Redis; journal/replay — бэклог) |
| At-least-once + ack | Y | Y | — | Y | Y |
| Exactly-once end-to-end | P | — | — | P | **P** (см. §2.3) |
| Порядок в partition/key | Y | P | — | Y (per subject/consumer) | **Y** (`!` + locks) |
| Priority queues | — | Y | — | — | **Y** (priority ZSET) |
| Delayed / scheduled | P | Y (plugin/TTL) | — | Y | **Y** (ZSET score) |
| Consumer groups / competing | Y | Y | Y (queue groups) | Y | **Y** (consumers + Get) |
| Prefetch / batch get | Y (max.poll) | Y (QoS) | — | Y | **Y** |
| Topic/exchange routing | Y | Y | Y (subjects) | Y | P (groups/partitions; selective groups — бэклог) |
| Fan-out / pub-sub | Y | Y | Y | Y | — (work queue; см. §2.3) |
| Dead-letter / retry delay | Y | Y | — | Y | **Y** (Reject + delay / reclaim) |
| Queue depth backpressure | P | Y | — | Y | **Y** (TagBackpressure) |
| Token / sliding / leaky rate limit в брокере | — | P | — | P | **Y** (5 algo, group+partition) |
| Cross-partition mutex | — | — | — | — | **Y** (`race!`) |
| Debounce / latest payload | — | — | — | — | **Y** (`deb!`) |
| Idempotency window после success | P | — | — | P | **Y** (Ack TTL) |
| Payload compression | Y | Y | Y | Y | **Y** (zstd, opt-in) |
| Horizontal scale | Y (брокеры) | Y | Y | Y | **Y*** (= Redis: multi-DB / Cluster / shards) |
| Multi-tenant isolation | Y | Y | Y | Y | P (queue name / DB; см. §2.3) |
| UI / ops tooling | Y | Y | P | P | **Y** (CLI + Wails) |

\*Не «без Redis»: координация всегда через Redis; рост throughput = topology Redis + шардинг очередей.

### 2.3 Уточнения по спорным строкам

**Exactly-once.** У Kafka EOS = транзакции брокера. У SRQ доставка **at-least-once** (Get → Ack; при падении — reclaim). Эффективный e2e once по *бизнесу* собирается: тот же `Task.ID` + F1 idempotency TTL и/или идемпотентный handler/side-effect + при необходимости ordered `!`. Гарантий «брокер сам сделал EOS без идемпотентности handler» нет — как у большинства очередей. В матрице **P**, не «нет».

**Fan-out / pub-sub.** Это *broadcast*: один publish → много независимых подписчиков, каждый получает свою копию. Не про постоянный TCP к Redis и не про audit-лог. SRQ = competing consumers (одну задачу берёт один воркер) → в матрице «—», пока нет явного copy/broadcast режима.

**Horizontal scale.** Масштаб SRQ = масштаб Redis (и клиентов). Multi-DB (0…11), разные инстансы, Cluster — как раз рычаги; Lua должна оставаться в рамках keyslot/дизайна ключей. Формулировка «без shared Redis» из Kafka-сравнения к нам не применима как минус продукта.

**Multi-tenant.** Изолируем tenant’ов друг от друга: данные (не читать чужие задачи), шум (квоты CPU/памяти/RL), ops (UI/ACL). Сейчас мягко: разные queue names / Redis DB / ACL приложения. Жёсткий SaaS-tenant слой — только по продуктовому запросу (см. ROADMAP §10).

### 2.4 Позиционирование SRQ (кратко)

**Сильные стороны:** ordered+reclaim; groups/blocked; Get-side dual RL; debounce; `race!`; tag backpressure; zstd.

**Не цель сейчас:** Kafka-like log replay (бэклог journal); fan-out broadcast; scale «без Redis».

---

## 3. Сценарные бенчи (где расти)

Исходники: `scenarios_bench_test.go` + prefetch в `queue_test.go`.  
Сырьё: `docs/bench-results/HEAD.txt`.

### 3.1 Prefetch

| Prefetch | Медиана (cross-commit HEAD) | Повтор `HEAD.txt` |
|----------|----------------------------:|------------------:|
| 1 (`BenchmarkConsume`) | ~236 µs | ~284 µs (шум) |
| 10 | ~204 µs | ~213 µs |

**Вывод:** prefetch даёт ~10–15% на лёгком payload; дальше Lua + Redis RTT. Имеет смысл гнать prefetch 1/5/10/50 на prod-like ordered+groups.

### 3.2 Размер payload (raw Publish)

| Size | ns/op | MB/s | allocs |
|-----:|------:|-----:|-------:|
| 128 B | 168 µs | 0.8 | 16 |
| 1 KiB | 169 µs | 6.1 | 16 |
| 16 KiB | 213 µs | 63 | 16 |
| 64 KiB | 295 µs | 227 | 16 |
| 256 KiB | 616 µs | 420 | 16 |

До ~1–16 KiB доминирует Redis/Lua fixed cost; дальше линейный рост от размера value.

### 3.3 Full cycle Publish→Get→Ack (raw)

| Size | ns/op | B/op |
|-----:|------:|-----:|
| 128 B | 647 µs | 2.4 KiB |
| 1 KiB | 605 µs | 4.3 KiB |
| 16 KiB | 671 µs | 36 KiB |
| 64 KiB | 789 µs | 142 KiB |

Аллокации на цикле растут с копированием payload из Redis — кандидат на buffer reuse / zero-copy decode.

### 3.4 Compression (zstd vs raw) — Publish

Payload = повторяющийся JSON-подобный паттерн (хорошо жмётся). Метрика `redis_B/op` — оценка размера value в Redis после Publish.

| Size | Mode | ns/op | redis_B/op | CPU allocs |
|-----:|------|------:|-----------:|-----------:|
| 1 KiB | raw | 182 µs | 1360 | 696 B / 16 |
| 1 KiB | zstd | 197 µs | **160** | 1287 B / 18 |
| 16 KiB | raw | 216 µs | 20560 | 695 B / 16 |
| 16 KiB | zstd | 209 µs | **160** | 9.0 KiB / 18 |
| 64 KiB | raw | 417 µs | 82000 | 695 B / 16 |
| 64 KiB | zstd | **237 µs** | **160** | 33 KiB / 18 |
| 256 KiB | raw | 616 µs | 327760 | 694 B / 16 |
| 256 KiB | zstd | **341 µs** | **176** | 129 KiB / 18 |

**Выводы:**

- На хорошо сжимаемых ≥16–64 KiB zstd **ускоряет Publish** (меньше сеть/Redis memory bandwidth) при росте CPU allocs на encode.  
- Redis footprint падает на **~100–2000×** на этом паттерне — главный win для памяти Redis и репликации.  
- На 1 KiB выигрыш по памяти есть, по latency — почти ноль / чуть хуже (overhead encoder).  
- Плохо сжимаемые бинарники (уже gzip/jpeg) нужно мерить отдельно — там zstd может только добавить CPU.

### 3.5 Compression full cycle

| Size | Mode | ns/op | B/op |
|-----:|------|------:|-----:|
| 16 KiB | raw | 728 µs | 37 KiB |
| 16 KiB | zstd | 686 µs | 29 KiB |
| 64 KiB | raw | 909 µs | 141 KiB |
| 64 KiB | zstd | 759 µs | 109 KiB |

Decode на Get окупается на крупных телах; allocs на decode всё ещё заметны — зона роста (decoder pool уже частично есть у klauspost/zstd).

### 3.6 Ordered partitions (Publish + drain)

| Partitions | ns/op (цикл) | ≈ µs / partition |
|-----------:|-------------:|-----------------:|
| 10 | 3.0 ms | 300 |
| 100 | 28.7 ms | 287 |
| 500 | 152 ms | 304 |

Масштабирование почти линейное по числу ordered-партиций в одном drain — узкое место: обход ready/groups и per-partition Get, не размер payload.

### 3.7 Redis CPU / blocked connector (кратко)

См. §1.3. Главный сигнал: **BlockGroups** держит idle Get ~const по N; **part-only Reject** жрёт ~2× CPU и выдаёт лишние задачи. Slowlog-тест — `TestBlockedGroupGet_SlowlogBudget`.

### 3.8 Куда ещё добавить сценарии

| Сценарий | Зачем | Статус |
|----------|--------|--------|
| Prefetch × ordered × N groups | prod-like hot path | частично (есть prefetch + ordered отдельно) |
| Blocked group / idle reject + Redis CPU | prod connector MAX | **есть** (`scenarios_cpu_bench_test.go`) |
| Rate-limit deny path (Get empty, F4 RL) | стоимость «пустого» Get под RL | нет |
| Debounce in-flight overwrite | стоимость лишнего GET/SET | нет |
| Concurrency tags contention | HASH locks под конкуренцией | нет |
| Parallel consumers (уже есть `BenchmarkConsumeParallel`) | fairness / lock | есть в `queue_test.go` |
| Incompressible payload ± zstd | честный worst case | нет |
| Batch Publish 1/10/100/1000 | amortized Lua | batch 10/100 есть |
| a96f70e pre-groups partition scan | baseline до BlockGroups | **есть** (`TestPartitionScanLoad_S1000`, `BENCH_SCAN=1`) |

---

## 4. Сводка оценки

1. **Исторически:** `a96f70e` → `0959fb3` добавили groups/ready/block: **batch publish −34%**, single path почти без изменений. Дальнейшие opt (`script`, `:count`, ping) частично вернули batch, но не до baseline. **HEAD** ≈ паритет с prod по single Publish/Consume, batch всё ещё ~+37% к prod₀ / ≈ prod₁.  
2. **Redis CPU (connector):** с `0959fb3` idle Get под `BlockGroups` ≈ O(1) по N; IdleReject `block_group` ≈ **2.2×** дешевле `part_only` по `redis_cpu_us/op`. Это целевой выигрыш групп/ready, не видимый на empty-queue microbench.  
3. **Vs брокеры:** SRQ — не Kafka-replacement, а Redis work-queue с редкими first-class фичами (debounce, race!, dual RL, ordered reclaim). Матрица §2.2.  
4. **Сценарии:** zstd окупается с ~16 KiB на сжимаемых данных; prefetch помогает; ordered ≈ O(partitions); CPU-регрессии ловим §1.3 + slowlog-тестом.

---

## 5. Артефакты

| Путь | Содержание |
|------|------------|
| `scripts/bench-commits.sh` | multi-commit worktree runner (`BENCH_CPU=1` → `*-cpu.txt`) |
| `scenarios_bench_test.go` | payload / compress / ordered |
| `scenarios_cpu_bench_test.go` | blocked group / idle reject / Redis CPU + slowlog test |
| `docs/bench-results/<sha>.txt` | сырые core `go test -bench` |
| `docs/bench-results/<sha>-cpu.txt` | CPU / blocked-connector прогоны |
| `docs/bench-results/HEAD.txt` | полный сценарный прогон HEAD |
