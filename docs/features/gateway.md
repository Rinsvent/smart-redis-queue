# Gateway (opt-in)

Опциональный single-active процесс перед Redis. Два равноправных data-plane:

| Транспорт | Назначение |
|-----------|------------|
| **gRPC** (`--listen`, по умолчанию `127.0.0.1:50051`) | Go-клиенты (`gateway/client`) |
| **HTTP JSON** (`--http`, по умолчанию `127.0.0.1:50052`) | Клиенты без gRPC (PHP, curl и т.п.) |

На том же HTTP-порту — ops: health, overview, shutdown.  
Прямой Redis SDK не меняется. Admin UI / CLI к данным очереди ходят в Redis напрямую, не через gateway.

## Назначение

После успешного ответа клиенту gateway удерживает publish в RAM на окно потери Redis (≤1s) и пишет mutating ops в op-log. При недоступности Redis: Publish/Get отклоняются; Ack/Reject буферизуются и воспроизводятся после восстановления.

Требование к Redis: `appendonly yes` и `appendfsync everysec` или `always`. Иначе процесс не стартует (кроме `--skip-persist-check` для разработки).

## Запуск

```bash
manager gateway \
  --addr localhost:6379 \
  --listen 127.0.0.1:50051 \
  --http 127.0.0.1:50052 \
  --dump /var/lib/srq/gateway.dump
```

## HTTP data-plane

Все методы: `POST`, `Content-Type: application/json`.

| Path | Запрос | Ответ |
|------|--------|--------|
| `/v1/publish` | `{ "queue", "tasks":[{id,payload,...}] }` | `{ "ok": true }` |
| `/v1/get` | `{ "queue", "consumer_id", "prefetch" }` | `{ "tasks":[...] }` |
| `/v1/ack` | `{ "queue", "consumer_id", "task_id", ... }` | `{ "ok": true }` |
| `/v1/reject` | `{ "queue", "consumer_id", "task_id", ... }` | `{ "ok": true }` |

Поле `payload` — строка. `payload_encoding`: `utf8` (по умолчанию) или `base64`.  
В ответе Get payload всегда с `payload_encoding: "base64"`.

HTTP-коды: `400` — неверный запрос; `409` — recovering/draining; `503` — redis_down; `500` — прочее. Тело ошибки: `{ "ok": false, "error": "..." }`.

Клиенты без gRPC используют только HTTP. Прямой доступ к Lua SRQ из таких клиентов не предусмотрен.

### Пример (PHP)

```php
<?php
$base = 'http://127.0.0.1:50052';

function srq_post(string $path, array $body): array {
    global $base;
    $ch = curl_init($base . $path);
    curl_setopt_array($ch, [
        CURLOPT_POST => true,
        CURLOPT_HTTPHEADER => ['Content-Type: application/json'],
        CURLOPT_POSTFIELDS => json_encode($body, JSON_UNESCAPED_UNICODE),
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => 30,
    ]);
    $raw = curl_exec($ch);
    $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    if ($code >= 300) {
        throw new RuntimeException("SRQ HTTP $code: $raw");
    }
    return json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
}

srq_post('/v1/publish', [
    'queue' => 'orders',
    'tasks' => [[
        'id' => 'order-42',
        'partition' => 'user:7',
        'payload' => json_encode(['sku' => 'A'], JSON_UNESCAPED_UNICODE),
        'payload_encoding' => 'utf8',
        'groups' => ['shop:1'],
    ]],
]);

$got = srq_post('/v1/get', [
    'queue' => 'orders',
    'consumer_id' => 'php-worker-1',
    'prefetch' => 5,
]);
foreach ($got['tasks'] as $t) {
    $payload = base64_decode($t['payload']);
    // обработка...
    srq_post('/v1/ack', [
        'queue' => 'orders',
        'consumer_id' => 'php-worker-1',
        'task_id' => $t['id'],
    ]);
}
```

## HTTP ops

| Path | Метод | Назначение |
|------|-------|------------|
| `/health` | GET | liveness; `503` при `redis_down` / `failed` |
| `/api/overview` | GET | state, размеры op-log / loss-window |
| `/api/shutdown` | POST | graceful drain → dump → exit |

```bash
manager gateway status --http http://127.0.0.1:50052
manager gateway stop  --http http://127.0.0.1:50052
```

`SIGINT` / `SIGTERM` запускают тот же Drain. `kill -9` в v1 небезопасен (нет disk WAL).

## systemd

Unit-файл (пример):

```ini
[Unit]
Description=SRQ Gateway
After=network.target redis.service

[Service]
Type=simple
ExecStart=/usr/local/bin/manager gateway --addr 127.0.0.1:6379 --listen 0.0.0.0:50051 --http 127.0.0.1:50052 --dump /var/lib/srq/gateway.dump
ExecStop=/usr/local/bin/manager gateway stop --http http://127.0.0.1:50052
KillMode=mixed
TimeoutStopSec=30
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Команды:

```bash
sudo systemctl enable --now srq-gateway   # старт + автозапуск
sudo systemctl status srq-gateway         # процесс unit (active/inactive)
sudo systemctl stop srq-gateway           # ExecStop → HTTP /api/shutdown → Drain
sudo systemctl restart srq-gateway
```

Поведение `systemctl stop`:

1. Выполняется `ExecStop` → `POST /api/shutdown` → процесс делает Drain (dump при `--dump`) и выходит.
2. Если за `TimeoutStopSec` процесс ещё жив — systemd шлёт `SIGTERM` (тот же Drain), затем при необходимости `SIGKILL`.

`systemctl status` показывает только жизнь unit’а (процесс запущен или нет). Состояние очереди / Redis (`running`, `redis_down`, …) — через `manager gateway status` или `GET /health` / `/api/overview`.

Для probe в оркестраторах: `GET http://127.0.0.1:50052/health`.

## Go SDK (gRPC)

```go
import "github.com/Rinsvent/smart-redis-queue/gateway/client"

conn, err := client.Dial(ctx, "127.0.0.1:50051")
if err != nil { /* ... */ }
defer conn.Close()
_ = conn.Producer("orders").Publish(ctx, &redisqueue.Task{ID: "1", Payload: []byte("x")})
```

## Loss-window и op-log

| Событие | Loss-window | Op-log |
|---------|-------------|--------|
| Publish OK | до `publishOK + window` | append |
| Ack | publish удаляется | append |
| Reject | publish не продлевается | append + opts |

## Шарды

`Config.Shards` (`ShardConfig`): Publish через тот же роутинг, что SDK; Get/Ack/Reject — fan-in по шардам. Задание шардов из CLI-файла — в планах.

## Ограничения v1 / roadmap

- нет disk WAL (защита от kill -9 процесса gateway);
- один активный инстанс (без active/passive и multi-active);
- нет Admin RPC через gateway;
- rate-limit остаётся в Redis Lua;
- нет бенчмарка gateway vs прямой SDK в CI.

## См. также

- [Гарантии доставки](../delivery.md#redis-persist)
- [Journal](journal.md)
