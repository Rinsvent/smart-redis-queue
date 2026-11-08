# Gateway (opt-in)

Single-active процесс перед Redis: data-plane Publish/Get/Ack/Reject через gRPC. Прямой Redis SDK не меняется. **Admin UI / `manager` CLI к Redis — как раньше**, не через gateway.

## Зачем

Короткие blip’ы / рестарт Redis: после OK клиенту gateway держит publish в RAM на окно потери Redis (≤1s) и пишет mutating ops в единый op-log. При RedisDown — fail-closed на Publish/Get; Ack/Reject копятся и replay’ятся после подъёма.

Не заменяет [Journal](journal.md) (длинный внешний SoT) и не даёт HA нескольких gateway.

## Запуск

Redis должен быть с `appendonly yes` и `appendfsync everysec|always`. Иначе gateway не поднимется.

```bash
manager serve --addr localhost:6379 --listen 127.0.0.1:50051 --dump /var/lib/srq/gateway.dump
# dev без AOF:
manager serve --skip-persist-check --listen 127.0.0.1:50051
```

## SDK

```go
import "github.com/Rinsvent/smart-redis-queue/gateway/client"

conn, err := client.Dial(ctx, "127.0.0.1:50051")
prod := conn.Producer("orders")
_ = prod.Publish(ctx, &redisqueue.Task{ID: "1", Payload: []byte("x")})

cons := conn.Consumer("orders", "worker-1")
tasks, _ := cons.Get(ctx)
```

Пакеты: `gateway` (сервер), `gateway/client`, proto в `gateway/proto`.

## Loss-window и op-log

| Событие | Loss-window | Op-log |
|---------|-------------|--------|
| Publish Redis OK | держим task `lossWindow` | append |
| Ack | **удаляем** publish (не воскрешать) | append |
| Reject | **продлеваем** retention publish | append + полные opts |

Replay строго по offset. Recovery: Get закрыт → replay → Running. Graceful stop: Drain → dump файла.

## Не в v1

- Disk WAL / kill-9 gateway (roadmap)
- Multi-active / active-passive gateway
- Admin RPC / прокси UI через gateway
- Вынос rate-limit в gateway (остаётся в Redis Lua)
- OK клиенту до записи в Redis

## См. также

- [Гарантии доставки](../delivery.md#redis-persist)
- [Journal](journal.md)
