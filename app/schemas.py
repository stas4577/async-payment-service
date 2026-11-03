from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator

PaymentStatus = Literal["pending", "succeeded", "failed"]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Literal["RUB", "USD", "EUR"]
    description: Annotated[str, Field(max_length=1000)]
    metadata: dict[str, Any]
    webhook_url: Annotated[AnyHttpUrl, Field(max_length=2048)]

    @field_validator("description", "metadata")
    @classmethod
    def reject_null_characters(cls, value: Any) -> Any:
        pending = [value]
        while pending:
            item = pending.pop()
            if isinstance(item, str) and "\x00" in item:
                raise ValueError("Strings must not contain null characters")
            if isinstance(item, dict):
                pending.extend(item.keys())
                pending.extend(item.values())
            elif isinstance(item, list):
                pending.extend(item)
        return value


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, Any]
    webhook_url: str
    processed_at: datetime | None


class PaymentEvent(BaseModel):
    payment_id: UUID
