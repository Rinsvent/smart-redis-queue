# Gateway (opt-in)

Single-active процесс перед Redis с **двумя равноправными data-plane**:

| Транспорт | Для кого |
|-----------|----------|
| **gRPC** (`--listen`, дефолт `:50051`) | Go SDK `gateway/client` |
| **HTTP JSON** (`--http`, дефолт `:50052`) | PHP, curl, любой язык без gRPC |

Плюс на том же HTTP — ops: health / overview / shutdown.  
Прямой Redis SDK не меняется. **Admin UI ходит в Redis напрямую**, не через gateway.

## Зачем

Короткие blip’ы Redis: после OK клиенту gateway держит publish в RAM на окно потери (≤1s) и пишет mutating ops в op-log. RedisDown → fail-closed Publish/Get; Ack/Reject копятся и replay’ятся.

## Запуск

```bash
manager gateway \
  --addr localhost:6379 \
  --listen 127.0.0.1:50051 \
  --http 127.0.0.1:50052 \
  --dump /var/lib/srq/gateway.dump

# dev без AOF:
manager gateway --skip-persist-check
```

Redis: `appendonly yes` + `appendfsync everysec|always`.

## HTTP API (data-plane) — для PHP

Все методы: `POST`, `Content-Type: application/json`.

| Path | Тело | Ответ |
|------|------|--------|
| `/v1/publish` | `{ "queue", "tasks":[{id,payload,...}] }` | `{ "ok": true }` |
| `/v1/get` | `{ "queue", "consumer_id", "prefetch" }` | `{ "tasks":[...] }` |
| `/v1/ack` | `{ "queue", "consumer_id", "task_id", ... }` | `{ "ok": true }` |
| `/v1/reject` | `{ "queue", "consumer_id", "task_id", delay?, dead_letter?, ... }` | `{ "ok": true }` |

**Payload:** на вход — строка; `payload_encoding`: `utf8` (default) или `base64`.  
В ответе Get всегда `payload_encoding: "base64"` (бинарно-безопасно).

### PHP (curl)

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

// Publish
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

// Get
$got = srq_post('/v1/get', [
    'queue' => 'orders',
    'consumer_id' => 'php-worker-1',
    'prefetch' => 5,
]);
foreach ($got['tasks'] as $t) {
    $payload = base64_decode($t['payload']);
    // ... обработать ...
    srq_post('/v1/ack', [
        'queue' => 'orders',
        'consumer_id' => 'php-worker-1',
        'task_id' => $t['id'],
    ]);
    // или reject:
    // srq_post('/v1/reject', [
    //   'queue' => 'orders', 'consumer_id' => 'php-worker-1', 'task_id' => $t['id'],
    //   'delay' => 5.0, 'dead_letter' => false,
    // ]);
}
```

**Да: из PHP очередь через gateway уже можно** — только HTTP, gRPC не нужен.  
Прямой доступ PHP→Redis к Lua SRQ не поддерживается; путь = HTTP gateway.

Коды ошибок: `400` (bad request), `409` (recovering/draining), `503` (redis_down), `500` (прочее). Тело: `{ "ok": false, "error": "..." }`.

## HTTP ops

| Path | Метод | Назначение |
|------|-------|------------|
| `/health` | GET | liveness; 503 при redis_down/failed |
| `/api/overview` | GET | state, op_log_len, … |
| `/api/shutdown` | POST | graceful drain+dump+exit |

```bash
manager gateway status --http http://127.0.0.1:50052
manager gateway stop  --http http://127.0.0.1:50052
```

SIGINT/SIGTERM — тот же Drain. `kill -9` в v1 без disk WAL небезопасен.

### systemd

```ini
[Service]
ExecStart=/usr/local/bin/manager gateway --addr 127.0.0.1:6379 --listen 0.0.0.0:50051 --http 127.0.0.1:50052 --dump /var/lib/srq/gateway.dump
ExecStop=/usr/local/bin/manager gateway stop --http http://127.0.0.1:50052
KillMode=mixed
TimeoutStopSec=30
```

## Go SDK (gRPC)

```go
import "github.com/Rinsvent/smart-redis-queue/gateway/client"

conn, _ := client.Dial(ctx, "127.0.0.1:50051")
_ = conn.Producer("orders").Publish(ctx, &redisqueue.Task{ID: "1", Payload: []byte("x")})
```

## Loss-window

| Событие | Loss-window | Op-log |
|---------|-------------|--------|
| Publish OK | до `publishOK + window` | append |
| Ack | удаляем publish | append |
| Reject | **не продлеваем** publish | append + opts |

## Шарды

Один gateway может держать `ShardConfig` (fan-in Get по шардам, Publish через тот же роутинг, что SDK). Конфиг пока в коде/`Config.Shards`; CLI-файл — дальше.

## Не в v1

- Disk WAL / kill-9
- Multi-active gateway
- Admin RPC через gateway
- Rate-limit в gateway (остаётся в Redis)

## См. также

- [Гарантии доставки](../delivery.md#redis-persist)
- [Journal](journal.md)
