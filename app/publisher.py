import asyncio
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import select

from app.database import engine, sessions
from app.models import Outbox
from app.rabbit import declare_queues, make_broker, new_payments

logger = logging.getLogger(__name__)


async def publish_one(broker: RabbitBroker) -> bool:
    async with sessions() as session, session.begin():
        event = await session.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None))
            .order_by(Outbox.created_at, Outbox.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if event is None:
            return False
        # A crash after confirm but before commit may publish twice. The consumer deduplicates.
        await broker.publish(
            {"payment_id": str(event.payment_id)},
            queue=new_payments,
            persist=True,
            mandatory=True,
            timeout=5,
            message_id=str(event.id),
        )
        event.published_at = datetime.now(UTC)
    return True


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    try:
        async with make_broker() as broker:
            await declare_queues(broker)
            while True:
                try:
                    if await publish_one(broker):
                        continue
                except Exception:
                    logger.exception("Outbox publication failed; event remains unpublished")
                await asyncio.sleep(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
