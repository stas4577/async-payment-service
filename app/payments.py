import hashlib
import json
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Outbox, Payment
from app.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    pass


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> Payment:
    normalized = data.model_dump(mode="json")
    normalized["amount"] = format(data.amount, ".2f")
    request_hash = hashlib.sha256(
        json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()

    async with session.begin():
        payment_id = await session.scalar(
            insert(Payment)
            .values(
                id=uuid4(),
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                details=data.metadata,
                webhook_url=str(data.webhook_url),
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment.id)
        )
        if payment_id is not None:
            session.add(Outbox(payment_id=payment_id))
            payment = await session.get(Payment, payment_id)
        else:
            # ON CONFLICT waits for the competing transaction; this SELECT sees its commit.
            payment = await session.scalar(
                select(Payment).where(Payment.idempotency_key == idempotency_key)
            )
            if payment.request_hash != request_hash:
                raise IdempotencyConflict
    return payment


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
