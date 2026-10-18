# Contributing

Спасибо за интерес к проекту.

## Как внести вклад

1. Fork репозитория
2. Ветка: `git checkout -b feature/amazing-feature`
3. Стиль: `gofmt`
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

Укажите:

- версию Go и Redis;
- минимальный repro;
- ожидаемое и фактическое поведение.

## Документация сайта

Страницы — Markdown в `docs/`, навигация — `mkdocs.yml`.

Локально (через [uv](https://github.com/astral-sh/uv) или venv):

```bash
uv venv .venv-docs
uv pip install -r requirements-docs.txt --python .venv-docs/bin/python
.venv-docs/bin/mkdocs serve
```

CI публикует GitHub Pages из `main` (workflow `docs.yml`).

## Фичи

Новые возможности — **opt-in**. Не ускоряйте hot path ценой обязательного оверхеда без флага. Идеи и приоритеты: [Roadmap](roadmap.md).
