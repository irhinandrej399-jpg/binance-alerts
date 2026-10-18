# Binance Alerts

Go-монорепозиторий backend-системы ценовых подписок.
Реализована первая версия Market Data: Binance WebSocket → PostgreSQL outbox → Kafka.
Пользователей, Alerts и Notifications пока нет. UI и gRPC не используются.

- [Архитектура Market Data](docs/architecture/market-data.md)
- [Домен приёма цены](docs/domains/market-data/price-ingestion.md)
- [Домен доставки outbox](docs/domains/market-data/outbox-relay.md)
- [Домен динамических инструментов](docs/domains/market-data/instrument-tracking.md)
- [Запуск и ограничения сервиса](services/market-data/README.md)

## Быстрый старт

Нужны Go 1.26+ и запущенный Docker с Compose.

```sh
make dev-up
make migrate
make run
```

Первые две команды создают локальные зависимости, топики и схему. `make run` подключается
к публичному потоку Binance по BTCUSDT и ETHUSDT; ключи не нужны.
Доступность Binance зависит от сети и региона. Внешнее соединение не требуется тестам.

```sh
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
make check
make integration
make dev-down
```

Readiness может быть 503 до подтверждения подписки или при недоступности источника/БД.
Остановка Compose сохраняет данные. Не удаляйте volume, если они нужны.
Настройки переопределяются окружением или локальным `.env` по образцу `.env.example`.
Локальные пароли демонстрационные; исходники, ключи и пользовательские документы
никуда не публикуются командами проекта.

`example.com/binance-alerts` — локальный путь Go-модуля, не адрес опубликованного репозитория.
Высокая пропускная способность пока не измерена. Потери до outbox и во время разрыва
Binance возможны: backfill отсутствует.

## Динамический набор инструментов

Для получения нужных пар через Kafka вместо фиксированного SYMBOLS:

```sh
make dev-up
make migrate
make run TRACKING_MODE=dynamic
```

При обновлении существующей установки повторите `make migrate`: добавлена миграция 002.
Команды приходят из `market.instrument-tracking.v1`, ключ — пара, значение —
InstrumentTrackingChanged v1. Пример и правила версий описаны в
[спецификации](docs/domains/market-data/instrument-tracking.md).
Alerts пока отсутствует; для локальной проверки команду можно отправить консольным producer
по инструкции сервиса. При пустом наборе сервис ожидает команды без подключения к Binance.
