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

Предпочтительно YAML-конфиг (шаблон: `gateway.example.yaml` в корне репозитория):

```bash
manager gateway --config /etc/srq/gateway.yaml
```

```yaml
listen: "127.0.0.1:50051"
http: "127.0.0.1:50052"
dump: "/var/lib/srq/gateway.dump"
# stats_retention: "1h"
redis:
  addr: "localhost:6379"
  db: 0
```

Флаги `--listen` / `--http` / `--dump` / `--skip-persist-check` перекрывают файл. Корневые `--addr` / `--password` / `--db` перекрывают секцию `redis` файла, если явно заданы (`IsSet`). Без `--config` — дефолты портов и Redis из флагов manager.

## HTTP data-plane

Все методы: `POST`, `Content-Type: application/json`.

| Path | Запрос | Ответ |
|------|--------|--------|
| `/v1/publish` | `{ "queue", "tasks", "tag_limits"?, "shard_key"?, "compression"? }` | `{ "ok": true }` |
| `/v1/get` | `{ "queue", "consumer_id", "prefetch" }` | `{ "tasks":[...] }` |
| `/v1/ack` | `{ "queue", "consumer_id", "task_id", idempotency/shared/open_blocks? }` | `{ "ok": true }` |
| `/v1/reject` | `{ "queue", "consumer_id", "task_id", delay?, block_*, dead_letter? }` | `{ "ok": true }` |

Поле `payload` — строка. `payload_encoding`: `utf8` (по умолчанию) или `base64`.  
В ответе Get payload всегда с `payload_encoding: "base64"`.

Опции Publish: `tag_limits` (map tag→max, backpressure → HTTP `429`), `shard_key` / `shard_name`, `compression` (`zstd`|`none`), `compress_min_bytes`.

HTTP-коды: `400` — неверный запрос; `409` — recovering/draining; `429` — backpressure; `503` — redis_down; `500` — прочее. Тело ошибки: `{ "ok": false, "error": "..." }`.

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
ExecStart=/usr/local/bin/manager gateway --config /etc/srq/gateway.yaml
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
| Ack | publish удаляется (обработано — не воскрешать) | append |
| Reject | publish **остаётся** до конца окна, RetainUntil **не** продлевается | append + opts |

Reject ≠ «готово»: если Redis потерял AOF за окно, recover делает re-publish из loss-window (тот же task id; `AlreadyExist` при целых данных — ok). Delay/blocks из Reject применяются через replay op-log, только пока задача ещё в processing (`RedisOK=false`).

## Heartbeat и stats

Consumer’ы через gateway создаются с `WithPing(false)`. Heartbeat выполняет сам gateway (~30s) по всем активным consumer’ам.  
При recovery перед replay op-log вызывается Ping (fence locks), чтобы reclaim не обогнал отложенные Ack.  
`stats_retention` в конфиге включает централизованный writeback счётчиков g/a/r в Redis.

## Шарды

Секция `shards` в YAML или `Config.Shards`: Publish с тем же роутингом, что SDK; Get/Ack/Reject — fan-in по шардам.

## Бенчмарк

```bash
go test ./gateway -run='^$' -bench='BenchmarkGatewayVsDirect' -benchtime=2s -count=3
```

In-process gateway API vs прямой `Producer`/`Consumer` (тот же Redis, без сетевого hop gRPC/HTTP). Замер `2026-11-08`, AMD Ryzen 7 2700, `count=3` (медиана ns/op):

| Сценарий | Direct | Gateway | Отношение | Вывод |
|----------|--------|---------|-----------|--------|
| Publish 256B | ~174 µs | ~205 µs | ~1.18× | **ниже** (~15% медленнее) |
| Publish 16KB | ~242 µs | ~274 µs | ~1.13× | **ниже**, тот же порядок |
| FullCycle 256B (pub+get+ack) | ~657 µs | ~668 µs | ~1.02× | **та же** |
| FullCycle 16KB | ~797 µs | ~788 µs | ~0.99× | **та же** |

Пропускная способность data-plane через gateway: для чистого Publish слегка ниже (op-log + loss-window), для полного цикла — в пределах шума / того же порядка. Сетевой gRPC/HTTP hop в бенч не входит.

## Ограничения / roadmap

- disk WAL / защита от kill -9 процесса gateway;
- active/passive и multi-active;
- Admin RPC через gateway;
- rate-limit admission остаётся в Redis Lua (не выносится в gateway).

## См. также

- [Гарантии доставки](../delivery.md#redis-persist)
- [Journal](journal.md)
