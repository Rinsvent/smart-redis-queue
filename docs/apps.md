# Приложения и сервер

Одна ops-поверхность, несколько способов запуска.

## CLI Manager

```bash
make manager
./bin/manager --help
```

| Команда | Описание |
|---------|----------|
| `info` (`list`, `ls`) | Очереди: партиции, pending, in-progress, consumers |
| `purge` | Очистить очередь или партицию |
| `retry` | Вернуть in-progress после падения consumer |
| `seed` | Демо-данные на нескольких DB |
| `publish` / `consume` | Поток для живого UI + stats |
| `desktop` | GUI (лучше собирать через `make desktop`) |
| `server` | Web UI (сборка `make server`) |

```bash
./bin/manager info
./bin/manager info -q my-queue -p "!acct_9f3a:doc_1"
./bin/manager purge -q my-queue
./bin/manager retry -q my-queue
./bin/manager seed --dbs 0,1,2
```

Флаги: `-a` addr, `-P` password, `--db`. Env: `REDIS_ADDR`, `REDIS_PASSWORD`.

## Web server

Тот же frontend, что Desktop, без CGO/GTK — удобно на сервере и в CI:

```bash
make server
./bin/manager-server server --host 127.0.0.1 --port 34115
# Health: http://127.0.0.1:34115/health
```

Для remote UI за прокси при необходимости: `--allow-any-origin` (осторожно в проде).

Сборка идёт с `-tags server` (не путать с desktop `-tags wails`).

## Desktop UI (Wails v3)

GUI для Redis-хостов:

- профили подключений (`~/.config/smart-redis-queue/connections.json` на Linux);
- каталог очередей по всем DB (SCAN);
- режимы Partitions / Groups / Tags;
- purge, block/unblock, retry, payload по ID, операции по тегам;
- график stats + watch тегов.

```bash
make desktop
# или релизы GitHub: *-desktop-*
```

Linux: GTK3 / WebKit. Windows: WebView2. macOS: Gatekeeper может спросить подтверждение.

## Mobile (экспериментально)

Android APK и iOS simulator-сборки из CI/релизов. На телефоне Redis — не `localhost` хоста разработки: укажите доступный адрес в профиле.

## Что чем собирать

| Артефакт | Make / теги | CGO |
|----------|-------------|-----|
| Библиотека / тесты | `go test ./...` | нет |
| CLI | `make manager` | нет |
| Web server | `make server` (`server`) | нет |
| Desktop | `make desktop` (`wails`, Linux + `gtk3`) | да |
| Android / iOS | `build/android`, `build/ios` Taskfile | NDK / Xcode |

Подробности и asset’ы релизов — в [README на GitHub](https://github.com/Rinsvent/smart-redis-queue).
