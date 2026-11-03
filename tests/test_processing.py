import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text

from app import consumer
from app.database import engine, sessions
from app.models import Outbox, Payment
from app.publisher import publish_one
from app.rabbit import new_payments


async def wait_for_payment(payment_id, predicate):
    async with asyncio.timeout(15):
        while True:
            async with sessions() as session:
                payment = await session.get(Payment, UUID(payment_id))
                if predicate(payment):
                    return payment
            await asyncio.sleep(0.05)


async def get_dlq(queue):
    async with asyncio.timeout(15):
        while True:
            message = await queue.get(fail=False)
            if message is not None:
                await message.ack()
                return message
            await asyncio.sleep(0.05)


@pytest.mark.parametrize("status", ["succeeded", "failed"])
async def test_outbox_to_webhook_and_duplicate(
    rabbit, payment_id, webhook, worker, monkeypatch, status
):
    gateway = AsyncMock(return_value=status)
    monkeypatch.setattr(consumer, "emulate_gateway", gateway)
    publisher, _, _ = rabbit
    assert await publish_one(publisher)
    payment = await wait_for_payment(payment_id, lambda p: p.webhook_sent_at is not None)
    assert payment.status == status
    assert payment.processed_at is not None
    assert payment.webhook_attempts == 1
    assert webhook[0][0]["body"] == {
        "payment_id": payment_id,
        "status": status,
        "processed_at": payment.processed_at.isoformat(),
    }
    assert webhook[0][0]["headers"]["idempotency-key"] == payment_id
    async with sessions() as session:
        outbox = await session.scalar(select(Outbox))
        assert outbox.published_at is not None
    # A late duplicate is harmless even when the original ACK has already been sent.
    await publisher.publish({"payment_id": payment_id}, queue=new_payments, persist=True)
    await asyncio.sleep(0.3)
    assert gateway.await_count == 1
    assert len(webhook[0]) == 1
    assert not await publish_one(publisher)


async def test_webhook_retries_then_succeeds(rabbit, payment_id, webhook, worker, monkeypatch):
    webhook[1]["count"] = 2
    gateway = AsyncMock(return_value="succeeded")
    monkeypatch.setattr(consumer, "emulate_gateway", gateway)
    await publish_one(rabbit[0])
    payment = await wait_for_payment(payment_id, lambda p: p.webhook_sent_at is not None)
    assert payment.webhook_attempts == 3
    assert len(webhook[0]) == 3
    assert gateway.await_count == 1


async def test_webhook_failure_dlq_and_no_reprocessing(
    rabbit, payment_id, webhook, worker, monkeypatch
):
    webhook[1]["count"] = 100
    gateway = AsyncMock(return_value="succeeded")
    monkeypatch.setattr(consumer, "emulate_gateway", gateway)
    await publish_one(rabbit[0])
    message = await get_dlq(rabbit[2])
    assert json.loads(message.body)["payment_id"] == payment_id
    assert message.headers["error-type"] == "WebhookDeliveryError"
    assert message.headers["consumer_attempts"] == 1
    assert message.headers["webhook_attempts"] == 3
    assert "attempts" not in message.headers
    payment = await wait_for_payment(payment_id, lambda p: p.webhook_attempts == 3)
    assert payment.status == "succeeded"
    assert payment.processed_at is not None
    assert payment.webhook_sent_at is None
    await rabbit[0].publish({"payment_id": payment_id}, queue=new_payments, persist=True)
    duplicate = await get_dlq(rabbit[2])
    assert duplicate.headers["consumer_attempts"] == 1
    assert duplicate.headers["webhook_attempts"] == 3
    assert gateway.await_count == 1
    assert len(webhook[0]) == 3


async def test_unreachable_webhook_goes_to_dlq(rabbit, payment_id, worker, monkeypatch):
    monkeypatch.setattr(consumer, "emulate_gateway", AsyncMock(return_value="failed"))
    async with sessions() as session, session.begin():
        payment = await session.get(Payment, UUID(payment_id))
        payment.webhook_url = "http://127.0.0.1:1/webhook"
    await publish_one(rabbit[0])
    message = await get_dlq(rabbit[2])
    assert message.headers["error-type"] == "WebhookDeliveryError"
    assert "ConnectError" in message.headers["error"]
    async with sessions() as session:
        payment = await session.get(Payment, UUID(payment_id))
        assert payment.status == "failed"
        assert payment.webhook_attempts == 3


async def test_resume_notification_after_saved_result(
    rabbit, payment_id, webhook, worker, monkeypatch
):
    gateway = AsyncMock(side_effect=AssertionError("Completed payment must not run again"))
    monkeypatch.setattr(consumer, "emulate_gateway", gateway)
    processed_at = datetime.now(UTC)
    async with sessions() as session, session.begin():
        payment = await session.get(Payment, UUID(payment_id))
        payment.status = "succeeded"
        payment.processed_at = processed_at
        payment.webhook_attempts = 1
    await publish_one(rabbit[0])
    payment = await wait_for_payment(payment_id, lambda p: p.webhook_sent_at is not None)
    assert payment.processed_at == processed_at
    assert payment.webhook_attempts == 2
    gateway.assert_not_awaited()
    assert len(webhook[0]) == 1


async def test_concurrent_duplicates_and_no_transaction_during_gateway(
    payment_id, webhook, monkeypatch
):
    async def gateway():
        async with engine.connect() as connection:
            transactions = await connection.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND pid <> pg_backend_pid() "
                    "AND state = 'idle in transaction'"
                )
            )
            assert transactions == 0
        await asyncio.sleep(0.1)
        return "succeeded"

    mock_gateway = AsyncMock(side_effect=gateway)
    monkeypatch.setattr(consumer, "emulate_gateway", mock_gateway)
    await asyncio.gather(*(consumer.process_payment(UUID(payment_id)) for _ in range(4)))
    assert mock_gateway.await_count == 1
    assert len(webhook[0]) == 1


async def test_technical_error_three_attempts_then_dlq(rabbit, payment_id, worker, monkeypatch):
    process = AsyncMock(side_effect=RuntimeError("Temporary processing error"))
    monkeypatch.setattr(consumer, "process_payment", process)
    await publish_one(rabbit[0])
    message = await get_dlq(rabbit[2])
    assert process.await_count == 3
    assert message.headers["consumer_attempts"] == 3
    assert "webhook_attempts" not in message.headers
    assert "attempts" not in message.headers
    assert message.headers["error-type"] == "RuntimeError"
    assert "Temporary processing error" in message.headers["error"]


async def test_technical_error_recovers(rabbit, payment_id, webhook, worker, monkeypatch):
    real_process = consumer.process_payment
    attempts = 0

    async def process(payment_id):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("Temporary database failure")
        await real_process(payment_id)

    monkeypatch.setattr(consumer, "process_payment", process)
    monkeypatch.setattr(consumer, "emulate_gateway", AsyncMock(return_value="succeeded"))
    await publish_one(rabbit[0])
    await wait_for_payment(payment_id, lambda p: p.webhook_sent_at is not None)
    assert attempts == 3
    assert len(webhook[0]) == 1


async def test_bad_message_goes_to_dlq(rabbit, worker):
    await rabbit[0].publish(b"not-json", queue=new_payments, persist=True)
    message = await get_dlq(rabbit[2])
    assert message.body == b"not-json"
    assert message.headers["error-type"] == "ValidationError"
    assert message.headers["consumer_attempts"] == 0
    assert "webhook_attempts" not in message.headers


async def test_unknown_payment_goes_to_dlq(rabbit, worker):
    await rabbit[0].publish({"payment_id": str(uuid4())}, queue=new_payments, persist=True)
    message = await get_dlq(rabbit[2])
    assert message.headers["error-type"] == "PaymentNotFound"


async def test_outbox_failure_keeps_event_unpublished(rabbit, payment_id, monkeypatch):
    monkeypatch.setattr(rabbit[0], "publish", AsyncMock(side_effect=TimeoutError("No confirm")))
    with pytest.raises(TimeoutError):
        await publish_one(rabbit[0])
    async with sessions() as session:
        assert (await session.scalar(select(Outbox))).published_at is None
    assert await rabbit[1].get(fail=False) is None


async def test_concurrent_publishers_do_not_take_same_event(rabbit, payment_id):
    results = await asyncio.gather(publish_one(rabbit[0]), publish_one(rabbit[0]))
    assert sorted(results) == [False, True]
    message = await rabbit[1].get()
    assert json.loads(message.body)["payment_id"] == payment_id
    await message.ack()
    assert await rabbit[1].get(fail=False) is None


async def test_broker_delivery_limit_is_bounded(rabbit):
    publisher, main, dlq = rabbit
    await publisher.publish({"payment_id": str(uuid4())}, queue=new_payments, persist=True)
    deliveries = 0
    async with asyncio.timeout(10):
        while True:
            message = await main.get(fail=False)
            if message is None:
                break
            deliveries += 1
            await message.reject(requeue=True)
            await asyncio.sleep(0.1)
    dead_message = await get_dlq(dlq)
    assert deliveries == 3
    assert dead_message.headers["x-death"][0]["reason"] == "delivery_limit"


async def test_dlq_publish_failure_falls_back_to_broker(rabbit, payment_id, worker, monkeypatch):
    process = AsyncMock(side_effect=consumer.WebhookDeliveryError("Attempts exhausted", 3))
    monkeypatch.setattr(consumer, "process_payment", process)
    monkeypatch.setattr(
        consumer.broker, "publish", AsyncMock(side_effect=TimeoutError("DLQ confirm lost"))
    )
    await publish_one(rabbit[0])
    message = await get_dlq(rabbit[2])
    assert json.loads(message.body)["payment_id"] == payment_id
    assert process.await_count == 3
    assert message.headers["x-death"][0]["reason"] == "delivery_limit"
