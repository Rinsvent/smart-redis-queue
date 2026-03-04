# Contributing

Спасибо за интерес к проекту!

## Как внести вклад

1. **Fork** репозитория
2. Создайте **ветку** (`git checkout -b feature/amazing-feature`)
3. Внесите изменения, сохраняя стиль кода (`gofmt`)
4. Добавьте **тесты** для новой функциональности
5. Запустите `make test`
6. Сделайте **commit** (`git commit -m 'Add amazing feature'`)
7. **Push** в ветку (`git push origin feature/amazing-feature`)
8. Откройте **Pull Request**

## Запуск тестов

Требуется Redis на `localhost:6379`. Быстрее всего поднять через Docker:

```bash
docker compose up -d
make test
```

## Сообщения об ошибках

При создании issue укажите:

- Версию Go
- Версию Redis
- Минимальный пример для воспроизведения
- Ожидаемое и фактическое поведение
