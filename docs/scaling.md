# Масштабирование через Redis

Масштаб SRQ **=** масштаб Redis и клиентов. Отдельного «кластера брокеров SRQ» нет — и это нормально для work-queue на Redis.

## Рычаги

1. **Больше consumer’ов / пулов** — unordered партиции и разные группы утилизируются параллельно.
2. **Несколько logical Redis DB (0…N)** — разнести очереди, снизить contention на одних ключах.
3. **Несколько инстансов Redis** — разные очереди/провайдеры API на разные хосты.
4. **Redis Cluster** — возможен при single-keyslot / осознанном key design для Lua.
5. **Меньше hot keys** — короткие блоки, Get-side RL, `BlockGroups` вместо шторма Reject, `race!` только где нужно.

## Groups / ready — критичный паттерн

При rate limit на аккаунт **паркуйте группу**, не каждую партицию ресурса:

```go
return redisqueue.NewRejectWithDelay(err, 2.0, "acct:9f3a")
```

Иначе Get обходит O(N) партиций → Redis CPU у потолка (исторически: Get ~77ms / сотни тысяч probe/s на staging).

С `BlockGroups` idle Get под запаркованной группой ≈ **O(1)** по числу партиций. Бенчи: [Бенчмарки](benchmarks.md).

## Multi-tenant (мягко)

| Уровень | Сейчас |
|---------|--------|
| Логический | разные имена `queue` |
| Redis | DB-per-tenant / instance-per-tenant |
| Квоты | RL / backpressure per group / tag |
| Ops | Redis ACL + фильтры в UI |

Жёсткий SaaS-tenant слой — только по продуктовому запросу (см. [Roadmap](roadmap.md)).

## Что не цель

- Scale «без shared Redis» как у Kafka brokers.
- Fan-out broadcast как способ «горизонтально размазать одно событие».

Для роста throughput смотрите topology Redis и правильную парковку групп, а не «ещё один брокер рядом».
