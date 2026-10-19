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

1. **Шардинг по Redis DB** — разнести очереди, снизить contention.
2. **Доработка Desktop UI** — по реальным болям из использования.
3. **Чеклист persistence** — что даёт Redis AOF/RDB и что даёт SRQ.
4. **Журнал / replay** — не Kafka-log из коробки; варианты archive, side-stream, CDC.
5. **Auto-ack** после успешного handler.
6. **Selective groups** — consumer читает только выбранные группы.
7. **Fan-out** — только если понадобится явный broadcast (это другая семантика).
8. **Заметки по Cluster / multi-DB** — углубить ops-гайд.
9. **Multi-tenant** — по запросу продукта; сейчас хватает queue / DB / ACL.

## Порядок

1. ~~Документация сайта~~  
2. Auto-ack  
3. Selective groups  
4. Шардинг по DB и заметки по Cluster  
5. Desktop и persistence checklist  
6. Journal / archive — после выбора модели  

Актуальный список в репозитории: [`ROADMAP.md`](https://github.com/Rinsvent/smart-redis-queue/blob/main/ROADMAP.md).
