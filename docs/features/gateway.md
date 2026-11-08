# Gateway (opt-in)

Single-active процесс перед Redis: **gRPC data-plane** (Publish/Get/Ack/Reject) + **HTTP ops** (health / status / shutdown). Прямой Redis SDK не меняется. **Admin UI / `manager` CLI к данным очереди — как раньше, напрямую в Redis**, не через gateway.

## Зачем

Короткие blip’ы / рестарт Redis: после OK клиенту gateway держит publish в RAM на окно потери Redis (≤1s) и пишет mutating ops в единый op-log. При RedisDown — fail-closed на Publish/Get; Ack/Reject копятся и replay’ятся после подъёма.

Не заменяет [Journal](journal.md) и не даёт HA нескольких gateway.

## Два порта

| Порт | Протокол | Назначение |
|------|----------|------------|
| `--listen` (дефолт `127.0.0.1:50051`) | **gRPC** | SDK: Publish / Get / Ack / Reject / Health RPC |
| `--http` (дефолт `127.0.0.1:50052`) | **HTTP** | Ops: health, overview, graceful shutdown |

Data-plane для приложений — только gRPC. HTTP нужен для мониторинга и остановки (аналог `rabbitmqctl status/stop`), не для publish/get.

## Запуск

Redis: `appendonly yes` и `appendfsync everysec|always`. Иначе boot падает с ошибкой.

```bash
manager gateway \
  --addr localhost:6379 \
  --listen 127.0.0.1:50051 \
  --http 127.0.0.1:50052 \
  --dump /var/lib/srq/gateway.dump

# dev без AOF:
manager gateway --skip-persist-check --listen 127.0.0.1:50051 --http 127.0.0.1:50052
```

## Обслуживание (ops)

### Статус

```bash
manager gateway status --http http://127.0.0.1:50052
# или
curl -s http://127.0.0.1:50052/api/overview | jq
curl -s http://127.0.0.1:50052/health
```

`/health` → 200 если state «здоровый»; 503 при `redis_down` / `failed`.

### Остановка

Предпочтительно graceful (drain → dump → exit):

```bash
manager gateway stop --http http://127.0.0.1:50052
# или
curl -X POST http://127.0.0.1:50052/api/shutdown
```

Также работают **SIGINT / SIGTERM** (systemd `systemctl stop` шлёт SIGTERM) — тот же Drain.

`kill -9` в v1 **не** гарантирует сохранность RAM-буфера (disk WAL — roadmap). Для prod: systemd + `stop`/`SIGTERM`, не `-9`.

### systemd (пример)

```ini
[Unit]
Description=SRQ Gateway
After=network.target redis.service

[Service]
ExecStart=/usr/local/bin/manager gateway --addr 127.0.0.1:6379 --listen 0.0.0.0:50051 --http 127.0.0.1:50052 --dump /var/lib/srq/gateway.dump
ExecStop=/usr/local/bin/manager gateway stop --http http://127.0.0.1:50052
Restart=on-failure
KillMode=mixed
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

`ExecStop` дергает HTTP shutdown; если процесс уже мёртв — systemd всё равно добьёт по Timeout. `KillMode=mixed` + SIGTERM на основном процессе тоже ок, если HTTP недоступен.

Проверка живости для мониторингов: `GET /health` или `manager gateway status`.

## SDK

```go
import "github.com/Rinsvent/smart-redis-queue/gateway/client"

conn, err := client.Dial(ctx, "127.0.0.1:50051") // gRPC, не HTTP
prod := conn.Producer("orders")
_ = prod.Publish(ctx, &redisqueue.Task{ID: "1", Payload: []byte("x")})

cons := conn.Consumer("orders", "worker-1")
tasks, _ := cons.Get(ctx)
```

Пакеты: `gateway` (сервер), `gateway/client`, proto в `gateway/proto`.

## Loss-window и op-log

| Событие | Loss-window (publish) | Op-log |
|---------|----------------------|--------|
| Publish Redis OK | держим до `publishOK + lossWindow` | append |
| Ack | **удаляем** publish (не воскрешать) | append |
| Reject | **не продлеваем** — окно только от publishOK; после истечения Redis уже на диске | append + полные opts (replay Reject, если потеряли только Reject) |

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
