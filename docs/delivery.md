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

### Сравнение с Kafka и RabbitMQ

У брокеров те же классы дыр, что у Redis: **OK продюсеру ≠ вечная сохранность**, и **consumer ack ≠ exactly-once**. Разница в том, *чем* закрывают окно потери — чаще **репликацией**, а не fsync на каждое сообщение (как `appendfsync always`).

| Риск | Kafka | RabbitMQ | Redis + SRQ |
|------|-------|----------|-------------|
| Publish/produce вернул OK, брокер убит | `acks=0` — можно потерять; `acks=1` — лидер принял, при падении лидера до реплик возможен **loss**; `acks=all` + достаточный `min.insync.replicas` — OK значит запись в ISR | Confirm без persistent/durable — слабо; persistent + durable queue — на брокере; classic queue всё ещё может потерять при падении узла до sync; **quorum queues** — OK после большинства | Без AOF — loss; `everysec` — до ~1s при power loss; `always` — на локальном диске; async replica — как отстающий follower |
| Машина / диск узла | Держатся на **репликах** (если ISR не сузили до одного); один брокер без реплик — потеря partition | Quorum / mirrored: нужна живая majority / зеркало; один узел — потеря | Нужны реплика Redis или бэкап AOF; один узел + смерть диска — потеря |
| Consumer обработал, ack, затем сбой | Offset закоммичен → не перечитают (при at-least-once до commit — дубль); EOS — отдельный режим транзакций | Ack снял сообщение с очереди; confirm/ack на диске зависит от типа очереди; дубли при redelivery до ack | Ack в Redis; если Ack не на диске — **повтор Get**; иначе нет |
| Дубль после сбоя | Стандартный at-least-once до commit offset; идемпотентный consumer / EOS | Redelivery до ack; идемпотентный handler | Reclaim / Ack не на диске; идемпотентный handler |

Окна потери (типичный prod, не «всё выключено»):

- **Kafka:** при `acks=all` и нормальном ISR окно на produce узкое; при `acks=1` или узком ISR — потеря последних записей лидера реальна. Fsync лога батчами — на durability обычно опираются на реплики, не на `always`-fsync каждой записи.
- **RabbitMQ:** publisher confirms + quorum (или аналог) — узкое окно; classic persistent на одном узле ближе к «приняли в память/журнал» с риском при краше до flush/реплики.
- **Redis + SRQ:** `appendfsync everysec` ≈ секундное окно при power loss (похоже по духу на «не ждать каждый fsync»); `always` — локально жёстче, чем дефолтный Kafka/Rabbit, но **дороже по throughput** (~37× в [измерениях](#измерения)); для отказоустойчивости узла всё равно нужна реплика (`WAIT` и т.п.), как ISR у Kafka.

Ни один из трёх не обещает «без потерь и без дублей» при любом restore из старого бэкапа или при async-реплике, которая не успела догнать.

*(Аналог «OK → fsync на локальный диск» из мира СУБД — MySQL `innodb_flush_log_at_trx_commit=1` ≈ Redis `appendfsync always`. У Kafka/Rabbit в проде чаще выбирают репликационную durability, а не такой flush на каждое сообщение.)*

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
