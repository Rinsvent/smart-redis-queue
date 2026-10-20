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

### 4. Persist / replay по offset (журнал)

Не Kafka-log из коробки. Варианты на оценку:

- **A.** Opt-in: вместо DEL после Ack — перекладка в `log`/`archive` очередь (или ZSET) с монотонным seq → можно «перечитать хвост».
- **B.** Side-stream: publish копии в отдельный consumer → Postgres/S3 (кто хочет audit).
- **C.** Внешний CDC/Redis stream mirror.

Нужен выбор модели (стоимость Redis RAM vs отдельное хранилище) и API replay.

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

### 8. Fan-out / pub-sub (уточнение)

**Что это:** одно сообщение получают *несколько независимых* подписчиков (каждый свой copy / свой offset). Это не «постоянный коннект к Redis» и не журналирование.

**Почему сейчас «—»:** SRQ — **work queue** (конкурирующие consumer’ы, одну задачу берёт один). Fan-out = другая семантика (broadcast).

**Если понадобится:** явный режим copy-on-publish в N очередей/групп, или отдельный pub-канал + воркеры-подписчики. Не смешивать с audit-log (§4).

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

---

## Порядок (ориентир)

1. ~~Документация: EOS-паттерн, fan-out vs work-queue, scale/multi-tenant~~ — сайт GitHub Pages (`docs/` + MkDocs): [rinsvent.github.io/smart-redis-queue](https://rinsvent.github.io/smart-redis-queue/). ANALYSIS остаётся детальным приложением.
2. Auto-ack (§6).
3. Selective groups / routing (§7).
4. Шардинг по DB (§1) + заметки Cluster.
5. Desktop (§2), persistence checklist (§3).
6. Persist/replay / archive (§4) — после выбора модели A/B/C.
