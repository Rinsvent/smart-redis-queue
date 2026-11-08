# Roadmap

Новые вещи по возможности включаются флагом. Горячий путь без опции не должен тяжелеть «за компанию».

## Уже сделано

- ordered-партиции и reclaim без ломания порядка;
- groups / ready / block;
- idempotency TTL после Ack;
- debounce (`deb!`);
- backpressure по тегам;
- rate limit на Get (несколько алгоритмов, группа и партиция);
- concurrency tags (`race!`);
- сжатие payload (zstd);
- Exchange (direct / fanout / topic) — [Exchange](features/exchange.md).

## В планах

1. **Доработка Desktop UI** — каталог/метрики по списку шардов, по болям из использования.
2. ~~**Чеклист persistence**~~ — см. [Гарантии доставки](delivery.md#redis-persist).
3. ~~**Журнал / replay**~~ — [Journal](features/journal.md): kafka/pg/mongo, PromoteDue, recovery Lock, `examples/journal`.
4. **Auto-ack** после успешного handler.
5. **Selective groups** — consumer читает только выбранные группы.
6. **Consistent hashing / live migration** — меньше боли при expand без полного drain.
7. **Multi-tenant** — по запросу продукта; сейчас хватает queue / DB / ACL.
8. **Script / queue flags** (TBD) — два уровня битовых флагов: L1 `queue:{name}:flags` (skip веток Get), L2 int в ARGV (mode Publish вместо `__srq_ex`/`__srq_jn`, bool-опции вызова). EX > JN > normal; без атомарного Exchange∩Journal. Ресерч: [`internal/docs/script-flags-design.md`](https://github.com/Rinsvent/smart-redis-queue/blob/main/internal/docs/script-flags-design.md). Пока **не берём в работу**.
9. **Gateway** — [gateway.md](features/gateway.md): `manager serve`, gRPC data-plane, RAM loss-window ≤1s, op-log, single-active. Дальше (roadmap): disk WAL / kill-9, active/passive, Admin RPC на gateway, multi-shard fan-in, async OK-after-WAL. Rate-limit **не** выносим из Redis.

## Порядок

1. ~~Документация сайта~~  
2. ~~Шардинг (ByGroup / ByQueue / ByManual, ReadOnly drain)~~ — [sharding.md](sharding.md)  
3. ~~Exchange (copy-on-publish)~~ — [exchange.md](features/exchange.md)  
4. Auto-ack  
5. Selective groups  
6. Desktop UI  
7. ~~Journal~~ — [journal.md](features/journal.md)  
8. Script / queue flags — после решения по ресерчу  
9. Gateway — доработки после v1 skeleton (disk WAL, HA, шарды)

Актуальный список в репозитории: [`ROADMAP.md`](https://github.com/Rinsvent/smart-redis-queue/blob/main/ROADMAP.md).
