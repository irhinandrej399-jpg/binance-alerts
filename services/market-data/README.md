# Market Data

## Реализовано

- Приём Binance Spot aggTrade через Gorilla WebSocket по SYMBOLS или долговечному набору инструментов.
- Kafka consumer команд отслеживания: версии, повторы, запоздавшие команды и сохранение отказов.
- Проверка и нормализация с точными строковыми ценами, сохранение в PostgreSQL outbox.
- Relay: ограниченный пакет, один активный publisher на БД, Sarama, Kafka ack перед commit.
- Повторы сохранения и публикации, reconnect/resubscribe, ping/pong и остановка по сигналу.
- Очистка опубликованных записей старше retention.
- Gorilla Mux: GET /healthz (liveness), GET /readyz (БД и состояние источника).

Нет HTTP API для рыночных сообщений. Нет реализации Alerts, каталога пар,
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
`internal/postgres/migrations/001_outbox.sql` и `002_instrument_tracking.sql`. Запуск не создаёт таблицы автоматически.
Последующие изменения схемы потребуют новых миграций, а не редактирования применённой.

## Настройки

| Переменная | Значение / смысл |
|---|---|
| DATABASE_URL | Обязательна; Makefile использует локальную демонстрационную БД |
| SYMBOLS | Обязателен только в static; Makefile: BTCUSDT,ETHUSDT |
| TRACKING_MODE | static или dynamic; по умолчанию static |
| TRACKING_TOPIC | market.instrument-tracking.v1 |
| TRACKING_GROUP | market-data.tracking.v1; одна группа на БД Market Data |
| TRACKING_REFRESH_INTERVAL | 1s; период сверки активного набора |
| BINANCE_WS_URL | wss://stream.binance.com:9443/ws; raw endpoint, миллисекунды |
| KAFKA_BROKERS | localhost:19092 |
| KAFKA_TOPIC | market.prices.v1 |
| HTTP_ADDR | 127.0.0.1:8080 |
| BATCH_SIZE | 10 (1…1000) |
| BATCH_TIMEOUT | 60s |
| POLL_INTERVAL | 100ms при пустой очереди |
| RETRY_DELAY | 1s после сбоя |
| OUTBOX_RETENTION | 24h; чистятся только опубликованные записи |

`make dev-up` создаёт топики цен и команд с 3 партициями и replication factor 1.
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

## Локальная проверка динамических подписок

После `make dev-up` и `make migrate` запустите `make run TRACKING_MODE=dynamic`.
В другом терминале откройте консольный producer:

```sh
docker compose -f deploy/compose/compose.yaml exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic market.instrument-tracking.v1 --property parse.key=true --property 'key.separator=|'
```

Введите одну строку:

```text
BTCUSDT|{"schema_version":1,"event_type":"InstrumentTrackingChanged","symbol":"BTCUSDT","enabled":true,"version":1}
```

Отключение — та же пара, enabled=false, version=2. Следующее включение требует version=3
или больше. При сохранённом состоянии выбирайте версию больше имеющейся; не сбрасывайте
её к 1 после перезапуска. Выйти из producer можно Ctrl+C.

Некорректные команды и конфликты версий сохраняются в tracking_rejections с координатами
Kafka и причиной; payload не сохраняется. При ошибке БД offset не отмечается, запись
повторяется. PostgreSQL — источник состояния после перезапуска; потеря БД требует
согласованного replay/reset offsets или snapshot, автоматического восстановления с нуля нет.

Readiness в dynamic учитывает доступность снимка состояния и готовность reader.
Пустой набор — ready idle. Недоступность Kafka сама по себе не обнуляет readiness:
сервис может принимать цены по уже сохранённому набору, а команды догонит позже.
При изменении набора reader заменяется; возможен пропуск сделок до переподключения.
Одна сессия поддерживает максимум 1024 пары, шардирования пока нет.
