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
- сжатие payload (zstd).

## В планах

1. **Доработка Desktop UI** — каталог/метрики по списку шардов, по болям из использования.
2. ~~**Чеклист persistence**~~ — см. [Гарантии доставки](delivery.md#redis-persist).
3. **Журнал / replay** — не Kafka-log из коробки; варианты archive, side-stream, CDC.
4. **Auto-ack** после успешного handler.
5. **Selective groups** — consumer читает только выбранные группы.
6. **Fan-out** — только если понадобится явный broadcast (это другая семантика).
7. **Consistent hashing / live migration** — меньше боли при expand без полного drain.
8. **Multi-tenant** — по запросу продукта; сейчас хватает queue / DB / ACL.

## Порядок

1. ~~Документация сайта~~  
2. ~~Шардинг (ByGroup / ByQueue / ByManual, ReadOnly drain)~~ — [sharding.md](sharding.md)  
3. Auto-ack  
4. Selective groups  
5. Desktop UI 
6. Journal / archive — после выбора модели  

Актуальный список в репозитории: [`ROADMAP.md`](https://github.com/Rinsvent/smart-redis-queue/blob/main/ROADMAP.md).
