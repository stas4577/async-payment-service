# Payment Processing Service

Небольшой сервис для асинхронной обработки платежей на FastAPI и RabbitMQ.

API принимает запрос на создание платежа, сохраняет его в PostgreSQL и возвращает `202 Accepted`. Дальнейшая обработка выполняется через очередь сообщений: Consumer эмулирует работу платёжного шлюза, обновляет статус платежа и отправляет результат на указанный webhook.

**Стек:** Python 3.12, FastAPI, Pydantic v2, SQLAlchemy 2.0 Async, PostgreSQL, RabbitMQ (FastStream), Alembic, httpx, Docker Compose.

## Запуск

Для запуска понадобятся Docker и Docker Compose.

```bash
cp .env.example .env
docker compose up --build
```

Параметры подключения, API-ключ и порты можно изменить в `.env`. По умолчанию используются значения для локального запуска.

Миграции Alembic применяются автоматически при старте.

После запуска:

- API: http://localhost:8000
- Swagger: http://localhost:8000/docs
- RabbitMQ Management: http://localhost:15672

PostgreSQL и AMQP доступны только внутри Docker-сети.

## API

### Создание платежа

`POST /api/v1/payments`

```bash
curl -i http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: local-payment-key' \
  -H 'Idempotency-Key: order-1001' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount": "125.50",
    "currency": "RUB",
    "description": "Order 1001",
    "metadata": {"order_id": "1001"},
    "webhook_url": "http://payments-webhook:8088/webhook"
  }'
```

Сервис возвращает `202 Accepted` с идентификатором платежа, текущим статусом и датой создания.

Поддерживаются RUB, USD и EUR. Сумма должна быть положительной, с точностью до двух знаков после запятой.

Обработка платежа занимает 2–5 секунд. Эмулятор возвращает успешный результат в 90% случаев, в остальных 10% платёж получает статус `failed`.

### Получение платежа

`GET /api/v1/payments/{payment_id}`

```bash
curl http://localhost:8000/api/v1/payments/PAYMENT_ID \
  -H 'X-API-Key: local-payment-key'
```

Ответ содержит данные платежа, его статус и время обработки.

Оба эндпоинта требуют заголовок `X-API-Key`.

### Идемпотентность

При создании платежа обязателен заголовок `Idempotency-Key`.

Повторный запрос с тем же ключом и теми же данными вернёт существующий платёж. Если использовать тот же ключ с другим телом запроса, API ответит `409 Conflict`.

Уникальность ключа обеспечивается на уровне PostgreSQL, включая конкурентные запросы.

## Проверка webhook

В проекте есть простой тестовый HTTP-приёмник. Его можно запустить в отдельном терминале:

```bash
docker compose run --build --rm --name payments-webhook tests \
  uvicorn tests.webhook_receiver:app --host 0.0.0.0 --port 8088
```

Для проверки доступны два адреса:

- `http://payments-webhook:8088/webhook` — принимает уведомление и возвращает HTTP 204.
- `http://payments-webhook:8088/unavailable` — всегда возвращает HTTP 503, позволяя проверить повторные попытки.

Адреса доступны из контейнеров проекта. Уведомления выводятся в терминал приёмника.

Webhook отправляется методом POST и содержит `payment_id`, `status` и `processed_at`.

При ошибке доставки выполняются три попытки с задержками 1 и 2 секунды. Если все попытки завершились неудачей, сообщение отправляется в DLQ. При этом уже обработанный платёж не проводится повторно.

## Как устроена обработка

Основная последовательность:

1. API сохраняет Payment и Outbox в одной транзакции PostgreSQL.
2. Publisher читает Outbox и публикует событие в `payments.new`.
3. Consumer получает событие и эмулирует обработку платежа.
4. Результат сохраняется в БД.
5. Consumer отправляет webhook и подтверждает обработку сообщения.

Для публикации используется **Outbox pattern**. Если RabbitMQ временно недоступен, событие остаётся в PostgreSQL и будет опубликовано после восстановления соединения.

Publisher использует `FOR UPDATE SKIP LOCKED` и подтверждения публикации RabbitMQ.

Доставка сообщений работает по принципу at-least-once, поэтому возможны дубликаты. Consumer учитывает текущее состояние платежа и не запускает повторную обработку уже завершённой операции.

Для синхронизации обработки одного платежа используется PostgreSQL advisory lock. Во время ожидания ответа шлюза и отправки HTTP-запросов транзакция БД не удерживается открытой.

Технические ошибки обрабатываются с повторными попытками. После исчерпания лимита сообщение попадает в `payments.dlq`.

Посмотреть состояние очередей можно через RabbitMQ Management или командой:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages messages_unacknowledged
```

## Тестирование

Интеграционные тесты запускаются с настоящими PostgreSQL и RabbitMQ в отдельном Docker Compose-проекте.

```bash
RABBITMQ_MANAGEMENT_PORT=15673 docker compose -p payments-test run --build --rm tests
```

Проверка линтером:

```bash
RABBITMQ_MANAGEMENT_PORT=15673 docker compose -p payments-test run --rm tests ruff check .
```

Тесты покрывают основные сценарии API, идемпотентность, Outbox, обработку сообщений, повторную доставку, webhook, retry и DLQ.

После тестирования окружение можно удалить:

```bash
docker compose -p payments-test down -v
```

Команда удаляет тестовые контейнеры и тома проекта `payments-test`.

## Ограничения

Проект реализован как тестовое задание, а не как production-ready платёжная система.

Платёжный шлюз эмулируется, используется один Consumer. Не реализованы подпись webhook, фильтрация адресов получателей, автоматическая обработка DLQ и очистка старых записей Outbox.

Доставка сообщений и webhook не гарантирует exactly-once. Например, если получатель обработал уведомление, но Consumer не успел сохранить подтверждение, уведомление может быть отправлено повторно. Для этого webhook содержит заголовок `Idempotency-Key` с идентификатором платежа.

В реальной интеграции с платёжным провайдером также потребовалась бы идемпотентность на стороне самого провайдера.
