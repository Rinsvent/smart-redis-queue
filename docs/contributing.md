# Contributing

Спасибо, что заглянули.

## Как внести вклад

1. Fork репозитория
2. Ветка: `git checkout -b feature/amazing-feature`
3. Код через `gofmt`
4. Тесты на новую функциональность
5. `make test`
6. Commit / push / Pull Request

## Тесты

Нужен Redis на `localhost:6379`:

```bash
docker compose up -d
make test
```

## Issues

Напишите версии Go и Redis, минимальный repro, что ожидали и что получили.

## Документация сайта

Страницы — Markdown в `docs/`, навигация — `mkdocs.yml`. Внутренний разбор бенчей по коммитам (`ANALYSIS.md`, `bench-results/`) в сайт не входит.

Локально:

```bash
uv venv .venv-docs
uv pip install -r requirements-docs.txt --python .venv-docs/bin/python
.venv-docs/bin/mkdocs serve
```

CI публикует GitHub Pages из `main` (workflow `docs.yml`).

## Фичи

Новое по возможности включайте флагом. Не ускоряйте hot path ценой обязательного оверхеда для всех. Идеи — в [Roadmap](roadmap.md).
