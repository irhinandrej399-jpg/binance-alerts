# Market Data

## Реализовано

- Приём Binance Spot aggTrade через Gorilla WebSocket по SYMBOLS.
- Проверка и нормализация с точными строковыми ценами, сохранение в PostgreSQL outbox.
- Relay: ограниченный пакет, один активный publisher на БД, Sarama, Kafka ack перед commit.
- Повторы сохранения и публикации, reconnect/resubscribe, ping/pong и остановка по сигналу.
- Очистка опубликованных записей старше retention.
- Gorilla Mux: GET /healthz (liveness), GET /readyz (БД и состояние источника).

Нет HTTP API для рыночных сообщений. Нет динамических команд от Alerts, каталога пар,
backfill, публичной аутентификации, метрик Prometheus или нагрузочных измерений.
Сервис выдаёт структурированные логи; payload целиком не логируется.

## Команды из корня

```sh
make dev-up
make migrate
make run
make check
make integration
make dev-down
```

Можно запускать напрямую: `go run ./services/market-data/cmd/market-data`.
Миграции: тот же путь с аргументом `migrate`; SQL встроен из
`internal/postgres/migrations/001_outbox.sql`. Запуск не создаёт таблицы автоматически.
Последующие изменения схемы потребуют новых миграций, а не редактирования применённой.

## Настройки

| Переменная | Значение / смысл |
|---|---|
| DATABASE_URL | Обязательна; Makefile использует локальную демонстрационную БД |
| SYMBOLS | Обязательный список пар через запятую; Makefile: BTCUSDT,ETHUSDT |
| BINANCE_WS_URL | wss://stream.binance.com:9443/ws; raw endpoint, миллисекунды |
| KAFKA_BROKERS | localhost:19092 |
| KAFKA_TOPIC | market.prices.v1 |
| HTTP_ADDR | 127.0.0.1:8080 |
| BATCH_SIZE | 10 (1…1000) |
| BATCH_TIMEOUT | 60s |
| POLL_INTERVAL | 100ms при пустой очереди |
| RETRY_DELAY | 1s после сбоя |
| OUTBOX_RETENTION | 24h; чистятся только опубликованные записи |

`make dev-up` создаёт топик с 3 партициями и replication factor 1.
Для production нужны отдельные решения по репликам, ISR, TLS/SASL и дисковым лимитам.
Долгое накопление pending outbox может заполнить диск.

## Тесты

`make check`: unit/локальные HTTP/WebSocket-тесты с race detector и go vet.
`make integration`: реальные PostgreSQL и Kafka; нужны поднятые зависимости.
Тесты создают временные SQL-схемы и Kafka-топик и удаляют только свои ресурсы.
Используйте отдельное тестовое окружение, не production.
Без TEST_DATABASE_URL/TEST_KAFKA_BROKERS интеграционные тесты завершаются ошибкой,
а не пропускаются незаметно. Makefile задаёт локальные значения.

Сквозной тест воспроизводит ack Kafka до неуспешного commit outbox и проверяет
повтор с тем же event_id. Exactly-once не обещается. Запись в outbox не восстанавливает
сообщения, пропущенные во время отключения Binance.

Контракты и правила: [цена](../../docs/domains/market-data/price-ingestion.md),
[outbox](../../docs/domains/market-data/outbox-relay.md),
[архитектура](../../docs/architecture/market-data.md).
