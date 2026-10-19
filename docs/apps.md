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
| `publish` / `consume` | Поток для живого UI и графика stats |
| `desktop` | GUI (удобнее собирать через `make desktop`) |
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

Тот же frontend, что у Desktop, но без CGO/GTK — удобно на сервере:

```bash
make server
./bin/manager-server server --host 127.0.0.1 --port 34115
# Health: http://127.0.0.1:34115/health
```

Для UI за прокси при необходимости есть `--allow-any-origin` (в проде включайте осознанно).

Сборка идёт с `-tags server`, не путать с desktop (`-tags wails`).

## Desktop UI (Wails v3)

GUI для Redis-хостов:

- профили подключений (`~/.config/smart-redis-queue/connections.json` на Linux);
- каталог очередей по всем DB;
- режимы Partitions / Groups / Tags;
- purge, block/unblock, retry, payload по ID, операции по тегам;
- график stats и watch тегов.

```bash
make desktop
# или готовые бинарники из GitHub Releases: *-desktop-*
```

Linux — GTK3 / WebKit, Windows — WebView2, на macOS Gatekeeper может спросить подтверждение.

## Mobile

Android APK и iOS simulator-сборки из CI/релизов. На телефоне Redis — не `localhost` машины разработки: в профиле укажите доступный адрес.

## Что чем собирать

| Артефакт | Как | CGO |
|----------|-----|-----|
| Библиотека / тесты | `go test ./...` | нет |
| CLI | `make manager` | нет |
| Web server | `make server` | нет |
| Desktop | `make desktop` (Linux + `gtk3`) | да |
| Android / iOS | Taskfile в `build/android`, `build/ios` | NDK / Xcode |

Подробности и ассеты релизов — в [README на GitHub](https://github.com/Rinsvent/smart-redis-queue).
