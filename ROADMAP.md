# Roadmap

Принципы новых фич: **opt-in only**; не раздувать горячий путь без флага; в README — честные имена.

---

## Сделано (кратко)

| ID | Что |
|----|-----|
| R1 | `:count` только для ordered (`!`) |
| R2 | блок группы vs пустой ready |
| **R3** | обход только свободных групп в Get (`ZRANGEBYSCORE -inf now` + LIMIT batches / random start в `collectOne`; блокированные score>`now` не обходятся). Сделано вместе с groups/ready/block; F4 RL на том же обходе ставит block group/partition. Остаточный edge (огромное число *пустых* свободных groups) — не актуален, не трекаем |
| R4/D4 | deferred future head в одном Get |
| R5 | batch индексов в ping; legacy fallback вырезан |
| F1 | Idempotency TTL после Ack |
| F2 | Debounce (`deb!`) |
| F3 | Tag backpressure на Publish |
| F4 | Rate limit в Get (5 algo, group ∧ partition) |
| F5 | Concurrency tags (`race!`, HASH locks) |
| F6 | Payload zstd (opt-in Producer) |

**Отменено:** F0 mega-script (`action=…` в одном EVAL). Shared Lua-фрагменты — только если появится боль от копипасты, не как отдельный эпик.

Бенч-базовая линия и сценарии: `docs/ANALYSIS.md`, `docs/bench-results/`, `scripts/bench-commits.sh`, `scenarios_bench_test.go`.

---

## Бэклог

### 1. Шардинг по Redis DB / инстансам — сделано (v1)

Публично: [`docs/sharding.md`](docs/sharding.md). Внутри: `internal/docs/sharding-design.md`.  
API: `ShardConfig`, `ShardByGroup|Queue|Manual|Partition`, `Producer.SetShards` / `ConsumerPool.SetShards`, `ReadOnly` drain, `KeyMap`, `WithShardKey` / `PublishTo`.  
Не в v1: Redis Cluster, авто-миграция ключей, consistent hashing.

### 2. Доработка Desktop UI (Wails)

Продолжение ops-поверхности: удобство по groups/tags/RL/blocked, payload, purge, профили хостов — по фактическим болям из использования.

### 3. Сохранность при падениях — сделано

Публично: [`docs/delivery.md`](docs/delivery.md) (чеклист AOF/RDB, сценарии Publish/Ack/inflight, сравнение с MySQL flush, бенчи always vs everysec).  
Тесты: `persistence_crash_test.go` (`SRQ_PERSIST_DOCKER=1`), конфиги `scripts/redis-persist/`.  
Сырые: `docs/bench-results/persistence-HEAD.txt`.

### 4. Persist / replay (журнал) — сделано

Публично: [`docs/features/journal.md`](docs/features/journal.md). Пример: `examples/journal`.

Durable SoT (kafka/pg/mongo); Redis cursor + `WithJournal`; ack отдельно; recovery = Lock из ack-топика + pump; `PromoteDue` (pg/mongo in-place, kafka ScheduleTopic); emergency `resume_after`.

### 5. Exactly-once (уточнение + возможные доработки)

Сейчас в матрице брокеров часто пишут «EOS = транзакции брокера». У нас другая модель, но **эффективный exactly-once результата** собирается:

- Get → обработка → Ack (at-least-once доставки);
- при ретрае — тот же `Task.ID` + F1 idempotency TTL и/или идемпотентный side-effect у handler;
- ordered `!` убирает гонки порядка в партиции.

Итого: брокер не даёт «транзакцию publish+consume как Kafka EOS», но **e2e once по бизнесу — да, если handler идемпотентен**. В бэклоге: явно задокументировать паттерн; опционально — helper/middleware «idempotent handler key», outbox к внешней БД. Не блокер протокола.

### 6. Auto-ack

Режим consumer/pool: после успешного `handler` без ошибки — Ack автоматически (сейчас часто ручной Ack в middleware/примере). Opt-in; при ошибке — Reject/policy.

### 7. Topic / exchange routing через группы (selective consume)

Воркеры читают **не все** группы, а allowlist/subscription (`SetGroups("conn:a", "conn:b")` или pattern). Реализация на текущей модели ready-per-group: Get обходит только выбранные свободные группы. Близко к «consumer подписан на subset routing keys».

### 8. Fan-out / pub-sub — ~~сделано~~ (`Exchange`)

Copy-on-publish в N bound очередей: `Exchange` (direct / fanout / topic), явный `Persist`, atomic EVAL. Документация: `docs/features/exchange.md`. Не Kafka CG / journal (§4).

### 9. Horizontal scale / Redis Cluster / multi-DB (уточнение)

Масштабирование SRQ **=** масштабирование Redis (и клиентов). «Без shared Redis» у Kafka означает: партиции живут на разных брокерах без единого in-memory state. У нас координация *всегда* через Redis — это нормально.

Что реально улучшает throughput:

- шардинг очередей по **DB 0…11** (§1) / разным инстансам;
- Redis Cluster (шарды ключей; Lua должна оставаться single-keyslot / осознанный key design);
- меньше hot keys (короткие блоки, RL, race locks).

В матрице лучше писать: scale = Redis topology + sharding, не «не масштабируется».

### 10. Multi-tenant isolation (уточнение → решение)

**Что изолируем:** tenant A не должен (а) читать/ack чужие задачи, (б) сожрать квоту Redis/CPU так, что падает B, (в) видеть ops/UI чужих очередей.

| Уровень | Сейчас | Возможный next |
|---------|--------|----------------|
| Логический | разные `queue` names | naming convention + ACL в приложении |
| Redis | общая DB, общие ключи `queue:*` | DB-per-tenant / instance-per-tenant (§1) |
| Квоты | RL/backpressure per group/tag | лимиты per-tenant prefix |
| Ops | Admin видит то, к чему есть Redis ACL | UI фильтр + Redis ACL users |

**Решение:** не отдельный протоколный эпик, пока нет жёсткого SaaS-multi-tenant. Берём как следствия §1 + документация «как изолировать»; жёсткий tenant middleware — только если появится продуктный запрос.

### 11. Script / queue flags (ресерч → TBD)

Два уровня битовых флагов вместо россыпи `"1"`/`"0"` и длинных `__srq_ex` / `__srq_jn`:

| Уровень | Где | Зачем |
|---------|-----|--------|
| **L1 system** | `queue:{name}:flags` | Publish OR-ит caps (`HAS_RACE`, `HAS_QP`, …); Get не заходит в лишние ветки / `redis.call` |
| **L2 call** | int в ARGV скрипта | Клиент: mode Publish + bool-опции вызова |

Mode Publish (L2): `MODE_EX` побеждает `MODE_JN`; оба выкл → обычный Publish. **Не** цель — атомарный Exchange∩Journal в одном вызове; только короткий флаг вместо маркера-строки и явный приоритет веток.

L1 v1: sticky OR + Rebuild/Purge (не auto-clear по depth=0). Контракт `SetConcurrencyTags` vs auto `HAS_RACE` — уточнить при реализации.

Статус: **не делаем**, пока нет решения «берём / откладываем». Ресерч: [`internal/docs/script-flags-design.md`](internal/docs/script-flags-design.md).

### 12. Gateway (v1 skeleton → доработки)

Публично: [`docs/features/gateway.md`](docs/features/gateway.md).  
Сейчас: `manager gateway --config`, gRPC + HTTP JSON data-plane (publish opts / backpressure), HTTP ops, RAM loss-window, op-log, recover+fence ping, централизованный heartbeat/stats, shards в YAML, бенч `BenchmarkGatewayVsDirect_*`.

Roadmap (не в ближайшей итерации): disk WAL / kill-9; active/passive / multi-active; Admin RPC через gateway.

---

## Порядок (ориентир)

1. ~~Документация: EOS-паттерн, fan-out vs work-queue, scale/multi-tenant~~ — сайт GitHub Pages (`docs/` + MkDocs): [rinsvent.github.io/smart-redis-queue](https://rinsvent.github.io/smart-redis-queue/). ANALYSIS остаётся детальным приложением.
2. Auto-ack (§6).
3. Selective groups / routing (§7).
4. Desktop (§2).
5. Script / queue flags (§11) — после решения по ресерчу; не в горячем порядке.
6. Gateway доработки (§12).
