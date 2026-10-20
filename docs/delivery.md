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
| **aof-always** | AOF, fsync на каждую запись до ответа клиенту | **Publish/Ack с OK переживают kill -9** процесса (проверено тестами). Не спасает от смерти диска / FS без реплики |

Для «не терять уже опубликованное» на одном узле: **`appendonly yes` + `appendfsync always`** (и volume/диск, которому доверяете).

Типичный prod-компромисс: **`appendfsync everysec`** + реплика; смириться с окном ~1s при аварии мастера до failover.

Реплика: `WAIT 1 0` (или политика приложения) после критичных записей усиливает «OK = есть на replica», но это уже отдельно от SRQ API.

### Сценарии (что бывает)

| Сценарий | no-persist / RDB без свежего save | aof-everysec | aof-always |
|----------|-----------------------------------|--------------|------------|
| Publish OK → Redis убит → restore | Сообщение **пропало**; продюсер думал, что ок | Обычно живо при kill процесса; при power loss возможен **loss** последних ~1s | **Живо** |
| Get, обработка, **ещё нет Ack** → Redis убит | При durable AOF payload на диске → после restore / expiry lock — **повторная выдача** (at-least-once) | то же | то же |
| **Ack OK** → Redis убит → restore | Без AOF БД пустая | Ack обычно на диске; иначе возможен **повтор** (обработали дважды) | Ack на диске → **повторной выдачи нет** |
| Воркер умер, Redis жив | reclaim по TTL lock — снова Get | — | — |

Итого по вашим вопросам:

1. **Publish OK → Redis упал → потеряли сообщение?**  
   Да, если нет durable AOF (или реплики, куда успело уйти). При **`appendfsync always`** в наших тестах (`kill -9` контейнера) сообщение **на месте**.

2. **Взяли задачу, Ack OK → Redis упал → снова обработали?**  
   При **`always`** — нет (Ack уже в AOF). При слабом persist / окне everysec + power loss — **да, возможен дубль** (классический at-least-once / «Ack не на диске»).

3. **Взяли, не Ack → Redis упал** — сообщение не «исчезает» при AOF; после восстановления его снова заберут (после lock TTL или если lock тоже восстановился и истечёт). Side-effect мог уже случиться → нужна идемпотентность handler.

### Сравнение с MySQL (уровень «как у БД»)

Идеальной «никогда не потеряем и никогда не дублируем» нет ни у MySQL, ни у Redis.

| Риск | MySQL InnoDB | Redis + SRQ |
|------|--------------|-------------|
| Commit/Publish вернул OK, процесс убит | При `innodb_flush_log_at_trx_commit=1` — на диске | При `appendfsync always` — на диске (аналог) |
| Commit OK, **машина** обесточилась | Зависит от flush + диск; =1 roughly durable | `always` ≈ то же; `everysec` ≈ потеря до ~1s (как `innodb_flush_log_at_trx_commit=2` по духу) |
| Восстановление из **старого дампа**/RDB | Потеря всего после бэкапа | То же для RDB/бэкапа AOF |
| Реплика отстаёт, мастер умер | Транзакции, не доехавшие в binlog на replica — **потеряны** (async) | Аналогично async replica; `WAIT` / sync replica — дороже |
| Дубль после сбоя | Клиент ретраит commit / app-level | Ack не на диске или reclaim → **повтор Get**; лечится идемпотентностью |

**Вывод:** с **`appendfsync always`** (и нормальным диском) Redis даёт **сопоставимый с MySQL `innodb_flush_log_at_trx_commit=1` уровень** «OK → запись на локальном диске». Это не защищает от потери диска без реплики/бэкапа и не убирает at-least-once дубли на стороне consumer. «Как восстановление из вчерашнего mysqldump» — и там, и там будут дыры.

---

## Результаты прогона (локально)

Среда: Ryzen 7 2700, Redis 6.2.6 в Docker, volume на хосте. Тесты: `TestPersist_*`, бенч: `BenchmarkPersistPublish` (`SRQ_PERSIST_DOCKER=1`).

### Crash (`kill -9` процесса Redis)

| Профиль | Publish пережил kill | Ack остался «съеденным» |
|---------|----------------------|-------------------------|
| no-persist | нет | — (БД пустая) |
| rdb-default | нет | — |
| aof-everysec | да* | да* |
| aof-always | **да** | **да** |

\*kill процесса ≠ power loss: байты AOF без fsync могут остаться в page cache хоста. Для everysec Redis официально допускает потерю ~1s при падении машины.

In-flight (Get без Ack) + aof-always: payload после рестарта **есть** → повторная обработка возможна после reclaim.

### Throughput Publish (медиана, `benchtime=2s`, count=3)

| Профиль | ns/op | vs no-persist |
|---------|------:|--------------:|
| no-persist | ~194 000 | 1.0× |
| rdb-default | ~260 000 | ~1.3× |
| aof-everysec | ~222 000 | ~1.1× |
| **aof-always** | **~7 120 000** | **~37× медленнее** |

`everysec` почти не бьёт publish на этой машине; **`always` — сильный trade-off** (fsync на каждый EVAL). Имеет смысл на критичном контуре или выносить durable Redis отдельно.

Сырые логи: `docs/bench-results/persistence-HEAD.txt` (не на Pages).

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
