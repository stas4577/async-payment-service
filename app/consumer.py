import asyncio
import logging
import random
from datetime import UTC, datetime
from uuid import UUID

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import engine
from app.models import Payment
from app.rabbit import dead_letters, declare_queues, make_broker, new_payments
from app.schemas import PaymentEvent

logger = logging.getLogger(__name__)
broker = make_broker()
app = FastStream(broker)
http_client = httpx.AsyncClient(timeout=httpx.Timeout(5), follow_redirects=False)


class WebhookDeliveryError(Exception):
    def __init__(self, message: str, webhook_attempts: int):
        super().__init__(message)
        self.webhook_attempts = webhook_attempts


class PaymentNotFound(Exception):
    pass


async def emulate_gateway() -> str:
    await asyncio.sleep(random.uniform(2, 5))
    return "succeeded" if random.random() < 0.9 else "failed"


async def deliver_webhook(session: AsyncSession, payment: Payment) -> None:
    payload = {
        "payment_id": str(payment.id),
        "status": payment.status,
        "processed_at": payment.processed_at.isoformat(),
    }
    while payment.webhook_attempts < 3:
        if payment.webhook_attempts:
            await asyncio.sleep(2 ** (payment.webhook_attempts - 1))
        # Persist the budget before HTTP: a process crash must not reset the retry counter.
        async with session.begin():
            payment.webhook_attempts += 1
        try:
            response = await http_client.post(
                payment.webhook_url,
                json=payload,
                headers={"Idempotency-Key": str(payment.id)},
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(
                "Webhook failed: payment=%s attempt=%s error=%s",
                payment.id,
                payment.webhook_attempts,
                type(exc).__name__,
            )
            if payment.webhook_attempts == 3:
                raise WebhookDeliveryError(
                    f"Webhook failed: {type(exc).__name__}", payment.webhook_attempts
                ) from exc
        else:
            async with session.begin():
                payment.webhook_sent_at = datetime.now(UTC)
            return
    raise WebhookDeliveryError("Webhook attempts exhausted", payment.webhook_attempts)


async def process_payment(payment_id: UUID) -> None:
    lock_key = int.from_bytes(payment_id.bytes[:8], "big", signed=True)
    async with engine.connect() as connection:
        try:
            # A session lock survives commits, without keeping a transaction open over I/O.
            await connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": lock_key})
            await connection.commit()
            async with AsyncSession(bind=connection, expire_on_commit=False) as session:
                async with session.begin():
                    payment = await session.get(Payment, payment_id)
                    if payment is None:
                        raise PaymentNotFound(f"Unknown payment: {payment_id}")
                if payment.status == "pending":
                    status = await emulate_gateway()
                    async with session.begin():
                        payment.status = status
                        payment.processed_at = datetime.now(UTC)
                if payment.webhook_sent_at is None:
                    await deliver_webhook(session, payment)
        finally:
            # Never return a pooled connection with a session lock still held.
            try:
                if not connection.invalidated:
                    await connection.rollback()
                    await connection.execute(
                        text("SELECT pg_advisory_unlock(:key)"), {"key": lock_key}
                    )
                    await connection.commit()
            except BaseException:
                await connection.invalidate()
                raise


async def send_to_dlq(message: RabbitMessage, error: Exception, consumer_attempts: int) -> None:
    headers = {
        **message.headers,
        "error-type": type(error).__name__,
        "error": str(error)[:512],
        "consumer_attempts": consumer_attempts,
        "failed-at": datetime.now(UTC).isoformat(),
        "original-queue": new_payments.name,
    }
    if isinstance(error, WebhookDeliveryError):
        headers["webhook_attempts"] = error.webhook_attempts
    try:
        await broker.publish(
            message.body,
            queue=dead_letters,
            persist=True,
            mandatory=True,
            timeout=5,
            message_id=message.message_id,
            headers=headers,
        )
    except Exception:
        logger.exception("DLQ publication failed; returning original message to broker")
        # The quorum queue delivery limit also bounds retries after crashes / DLQ failure.
        await message.reject(requeue=True)
    else:
        await message.ack()


@broker.subscriber(new_payments, ack_policy=AckPolicy.MANUAL)
async def consume(message: RabbitMessage) -> None:
    try:
        event = PaymentEvent.model_validate_json(message.body)
    except ValidationError as exc:
        await send_to_dlq(message, exc, consumer_attempts=0)
        return
    for attempt in range(1, 4):
        try:
            await process_payment(event.payment_id)
        except (WebhookDeliveryError, PaymentNotFound) as exc:
            await send_to_dlq(message, exc, consumer_attempts=attempt)
            return
        except Exception as exc:
            logger.exception("Processing error: payment=%s attempt=%s", event.payment_id, attempt)
            if attempt == 3:
                await send_to_dlq(message, exc, consumer_attempts=attempt)
                return
            await asyncio.sleep(2 ** (attempt - 1))
        else:
            # Both final status and successful webhook delivery have been committed before ACK.
            await message.ack()
            return


@app.on_startup
async def startup() -> None:
    await broker.connect()
    await declare_queues(broker)


@app.after_shutdown
async def shutdown() -> None:
    await http_client.aclose()
    await engine.dispose()
