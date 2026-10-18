# Roadmap

Принципы: **opt-in only**; не раздувать горячий путь без флага; честные имена в документации.

## Сделано

| ID | Что |
|----|-----|
| R1–R5 | `:count` для ordered, groups/ready/block, deferred head, ping batch |
| F1 | Idempotency TTL после Ack |
| F2 | Debounce (`deb!`) |
| F3 | Tag backpressure |
| F4 | Rate limit в Get (5 algo, group ∧ partition) |
| F5 | Concurrency tags (`race!`) |
| F6 | Payload zstd |

Отменено: F0 mega-script (один EVAL на все action).

## Бэклог

1. **Шардинг по Redis DB (0…N)** — меньше contention, квоты между DB.
2. **Desktop UI** — удобство groups/tags/RL/blocked по фактическим болям.
3. **Persistence checklist** — что гарантирует Redis AOF/RDB vs SRQ at-least-once.
4. **Persist / replay journal** — не Kafka-log OOTB; варианты archive / side-stream / CDC.
5. **Документация EOS-паттерна** (+ опциональный helper) — частично закрыто сайтом.
6. **Auto-ack** после успешного handler (opt-in).
7. **Selective groups** — consumer читает allowlist групп (topic-like).
8. **Fan-out** — только если понадобится явный broadcast (отдельная семантика).
9. **Cluster / multi-DB notes** — углубить ops-гайд.
10. **Multi-tenant** — по запросу продукта; сейчас queue/DB/ACL.

## Порядок (ориентир)

1. ~~Документация сайта~~ (этот сайт)  
2. Auto-ack  
3. Selective groups  
4. DB sharding + Cluster notes  
5. Desktop / persistence checklist  
6. Journal / archive — после выбора модели  

Актуальная версия в репозитории: [`ROADMAP.md`](https://github.com/Rinsvent/smart-redis-queue/blob/main/ROADMAP.md).
