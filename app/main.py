import secrets
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import engine, sessions
from app.payments import IdempotencyConflict, create_payment, get_payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetail


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


def require_api_key(x_api_key: Annotated[str, Header()] = "") -> None:
    if not secrets.compare_digest(x_api_key.encode(), settings.api_key.get_secret_value().encode()):
        raise HTTPException(status_code=401, detail="Invalid API key")


async def get_session():
    async with sessions() as session:
        yield session


app = FastAPI(title="Payment Processing Service", lifespan=lifespan)
Session = Annotated[AsyncSession, Depends(get_session)]


@app.post(
    "/api/v1/payments",
    status_code=202,
    response_model=PaymentAccepted,
    dependencies=[Depends(require_api_key)],
)
async def post_payment(
    data: PaymentCreate,
    session: Session,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255, pattern=r"\S")],
):
    try:
        payment = await create_payment(session, data, idempotency_key)
    except IdempotencyConflict as exc:
        raise HTTPException(
            status_code=409, detail="Idempotency key used with another body"
        ) from exc
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@app.get(
    "/api/v1/payments/{payment_id}",
    response_model=PaymentDetail,
    dependencies=[Depends(require_api_key)],
)
async def read_payment(payment_id: UUID, session: Session):
    payment = await get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return PaymentDetail(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.details,
        webhook_url=payment.webhook_url,
        processed_at=payment.processed_at,
    )
