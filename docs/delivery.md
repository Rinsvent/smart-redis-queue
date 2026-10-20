# Гарантии доставки

## At-least-once

Базовый цикл:

1. **Get** — задача выдана consumer’у, держится heartbeat’ом.
2. Успех → **Ack**.
3. Ошибка → **Reject** (можно с delay или парковкой группы).
4. Воркер умер → по истечении TTL lock (~120s) задача возвращается в очередь (**reclaim**).

Сообщение может обработаться больше одного раза, если Ack не успел, а reclaim уже вернул задачу. Handler должен переживать ретраи.

## Чего брокер не обещает

| Обещание | SRQ |
|----------|-----|
| At-most-once без потерь | Нет (по умолчанию at-least-once) |
| Exactly-once без идемпотентности handler | Нет |
| Транзакции publish+consume как у Kafka EOS | Нет |
| Fan-out каждому подписчику | Нет |

## Как получить «ровно один раз» по бизнесу

1. Стабильный `Task.ID` (бизнес-ключ).
2. Идемпотентный side-effect (upsert, unique constraint).
3. По желанию — `SetIdempotencyTtl`: после успеха повторный Publish того же ID не примется, пока жив TTL.
4. Ordered `!`, если важен порядок в партиции.

```go
consumer.SetIdempotencyTtl(24 * time.Hour)

consumer.Consume(ctx, func(task *redisqueue.Task) error {
    // upsert по task.ID — безопасно при повторной доставке
    return applyOnce(task)
})
```

## Порядок

- Ordered (`!`): один consumer на партицию; reclaim сохраняет исходный score.
- Prefetch: reject первой задачи возвращает и остальные из батча с тем же порядком.
- Unordered: параллелизм важнее порядка.

---

## Персист Redis: что гарантирует SRQ, что — Redis

SRQ хранит очередь **только в Redis**. Протокол Get/Ack/Reject/reclaim не подменяет durability движка.

| Слой | Гарантия |
|------|----------|
| **SRQ** | Пока ключи живы в Redis: at-least-once до Ack; после успешного Ack payload удаляется (или stub+TTL при idempotency). |
| **Redis** | Переживут ли Publish/Ack `kill -9` / power loss — зависит от AOF/RDB/реплики. |

Без подходящего persistence «Publish вернул OK» ≠ «запись на диске».

### Конфиги (чеклист)

Файлы в репозитории: `scripts/redis-persist/*.conf`.

| Профиль | Суть | Publish OK → kill процесса Redis → restart |
|---------|------|--------------------------------------------|
| **no-persist** (`save ""`, `appendonly no`) | Только RAM | **Потеря** |
| **rdb-default** | Редкие BGSAVE | **Обычно потеря**, если snapshot ещё не успел |
| **aof-everysec** | AOF, fsync раз в секунду | При **kill процесса** часто ок (page cache хоста); при **power loss / падении ОС** — до ~**1s** записей по доке Redis |
| **aof-always** | AOF, fsync на каждую запись до ответа клиенту | **Publish/Ack с OK переживают kill -9** процесса. Не спасает от смерти диска / FS без реплики |

Чтобы не терять уже опубликованные задачи на одном узле: **`appendonly yes` + `appendfsync always`** (и надёжный volume/диск).

Типичный prod-компромисс: **`appendfsync everysec`** и реплика; при аварии мастера до failover допустимо окно потери ~1s.

Реплика: после критичных записей приложение может вызывать `WAIT` (например `WAIT 1 0`), чтобы OK означал «есть на replica». Это вне API SRQ.

### Типовые сбои

| Сценарий | no-persist / RDB без свежего save | aof-everysec | aof-always |
|----------|-----------------------------------|--------------|------------|
| Publish OK → Redis убит → restore | Сообщение **потеряно**, хотя клиент получил OK | Обычно живо при kill процесса; при power loss возможен **loss** последних ~1s | **Сохранено** |
| Get без Ack → Redis убит | При durable AOF payload на диске → после restore / expiry lock — **повторная выдача** | то же | то же |
| Ack OK → Redis убит → restore | Без AOF БД пустая | Ack обычно на диске; иначе возможен **повтор** обработки | Ack на диске → **повторной выдачи нет** |
| Воркер умер, Redis жив | reclaim по TTL lock — снова Get | — | — |

Кратко по исходам:

- **Потеря после Publish OK** — возможна без durable AOF (или без реплики, куда запись успела уйти). При `appendfsync always` запись с успешным ответом клиенту остаётся после kill процесса Redis.
- **Повтор после Ack OK** — возможен, если Ack не попал на диск (`everysec` + power loss, или нет AOF). При `always` повторной выдачи после успешного Ack нет.
- **Get без Ack, затем падение Redis** — при AOF задача не исчезает; после восстановления её снова выдадут (после lock TTL). Side-effect handler мог уже выполниться — нужна идемпотентность.

### Сравнение с MySQL

Ни MySQL, ни Redis не дают абсолютной гарантии «без потерь и без дублей» во всех авариях.

| Риск | MySQL InnoDB | Redis + SRQ |
|------|--------------|-------------|
| Commit/Publish вернул OK, процесс убит | При `innodb_flush_log_at_trx_commit=1` — на диске | При `appendfsync always` — на диске |
| Commit OK, **машина** обесточилась | Зависит от flush и диска; `=1` обычно durable | `always` — сопоставимо; `everysec` — до ~1s потери (близко к `innodb_flush_log_at_trx_commit=2`) |
| Восстановление из **старого дампа**/RDB | Потеря всего после бэкапа | То же для RDB / старого AOF |
| Реплика отстаёт, мастер умер | Не доехавшие в binlog на replica транзакции **потеряны** (async) | То же для async replica; `WAIT` / sync replica дороже |
| Дубль после сбоя | Ретрай клиента / уровень приложения | Ack не на диске или reclaim → **повтор Get**; идемпотентный handler |

С **`appendfsync always`** и исправным диском Redis даёт уровень «OK → запись на локальном диске», сопоставимый с MySQL при `innodb_flush_log_at_trx_commit=1`. Это не замена реплики/бэкапа при отказе диска и не отменяет at-least-once на стороне consumer. Восстановление из устаревшего дампа оставляет ту же дыру, что и у MySQL.

---

## Измерения

Среда: Linux amd64, Ryzen 7 2700, Redis 6.2.6 в Docker, volume на хосте. Интеграционные тесты `TestPersist_*`, бенч `BenchmarkPersistPublish` (флаг `SRQ_PERSIST_DOCKER=1`).

### Crash (`kill -9` процесса Redis)

| Профиль | Publish пережил kill | Ack остался применённым |
|---------|----------------------|-------------------------|
| no-persist | нет | — (БД пустая) |
| rdb-default | нет | — |
| aof-everysec | да* | да* |
| aof-always | **да** | **да** |

\*Kill процесса не равен power loss: байты AOF без fsync могут остаться в page cache хоста. Документация Redis для `everysec` допускает потерю ~1s при падении машины.

Get без Ack при `aof-always`: после рестарта payload на месте — возможна повторная обработка после reclaim.

### Throughput Publish (медиана, `benchtime=2s`, `count=3`)

| Профиль | ns/op | vs no-persist |
|---------|------:|--------------:|
| no-persist | ~194 000 | 1.0× |
| rdb-default | ~260 000 | ~1.3× |
| aof-everysec | ~222 000 | ~1.1× |
| **aof-always** | **~7 120 000** | **~37× медленнее** |

`everysec` почти не снижает Publish относительно RAM-only; **`always`** — существенный trade-off (fsync на каждый EVAL). Для критичного контура либо принимают стоимость, либо выносят durable Redis отдельно.

```bash
SRQ_PERSIST_DOCKER=1 go test -run='TestPersist_' -count=1 -timeout=15m -v .

SRQ_PERSIST_DOCKER=1 go test -run='^$' -bench='BenchmarkPersistPublish' \
  -benchtime=2s -count=3 -timeout=30m .
```

Конфиги: `scripts/redis-persist/`.

---

## Рядом по смыслу

- [Rate limit / BlockGroups](features/rate-limit.md) — не терять задачи при 429, а парковать scope
- [Debounce](features/debounce-idempotency.md) — свежий payload вместо пачки устаревших
- [race!](features/race.md) — не два in-flight на один shared resource
