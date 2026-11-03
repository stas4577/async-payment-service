import asyncio
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event, func, select

from app.database import engine, sessions
from app.models import Outbox, Payment


async def test_create_get_and_atomic_outbox(api, body):
    response = await api.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "order"})
    assert response.status_code == 202
    result = response.json()
    assert result["status"] == "pending"
    payment_id = UUID(result["payment_id"])
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        outbox = await session.scalar(select(Outbox).where(Outbox.payment_id == payment_id))
        assert str(payment.amount) == "125.50"
        assert outbox is not None and outbox.published_at is None
    detail = await api.get(f"/api/v1/payments/{payment_id}")
    assert detail.status_code == 200
    assert detail.json()["metadata"] == body["metadata"]
    assert detail.json()["processed_at"] is None
    assert (await api.get(f"/api/v1/payments/{uuid4()}")).status_code == 404


async def test_idempotency_replay_conflict_and_concurrency(api, body):
    async def create(data):
        return await api.post(
            "/api/v1/payments", json=data, headers={"Idempotency-Key": "same-order"}
        )

    responses = await asyncio.gather(*(create(body) for _ in range(12)))
    assert all(response.status_code == 202 for response in responses)
    assert len({response.json()["payment_id"] for response in responses}) == 1
    assert (await create({**body, "amount": "125.5"})).status_code == 202
    conflict = await create({**body, "amount": "126"})
    assert conflict.status_code == 409
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_concurrent_different_bodies_conflict(api, body):
    responses = await asyncio.gather(
        *(
            api.post(
                "/api/v1/payments",
                json={**body, "amount": str(amount)},
                headers={"Idempotency-Key": "conflicting-order"},
            )
            for amount in [1, 2]
        )
    )
    assert sorted(response.status_code for response in responses) == [202, 409]


async def test_rollback_does_not_leave_payment_without_outbox(api, body):
    def reject_outbox(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO outbox"):
            raise RuntimeError("Simulated outbox write failure")

    event.listen(engine.sync_engine, "before_cursor_execute", reject_outbox)
    try:
        with pytest.raises(RuntimeError, match="outbox write failure"):
            await api.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "rollback"})
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", reject_outbox)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "incorrect"}])
async def test_both_endpoints_require_api_key(api, body, headers):
    api.headers.clear()
    assert (await api.get(f"/api/v1/payments/{uuid4()}", headers=headers)).status_code == 401
    response = await api.post(
        "/api/v1/payments", json=body, headers={**headers, "Idempotency-Key": "auth"}
    )
    assert response.status_code == 401


@pytest.mark.parametrize(
    "change",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"amount": "10000000000000000"},
        {"amount": "NaN"},
        {"currency": "GBP"},
        {"webhook_url": "file:///tmp/test"},
    ],
)
async def test_invalid_input(api, body, change):
    response = await api.post(
        "/api/v1/payments", json={**body, **change}, headers={"Idempotency-Key": "invalid"}
    )
    assert response.status_code == 422


@pytest.mark.parametrize(
    "field,value",
    [
        ("description", "Order\x00123"),
        ("metadata", {"value": "bad\x00value"}),
        ("metadata", {"nested": {"value": "bad\x00value"}}),
        ("metadata", {"items": ["valid", "bad\x00value"]}),
        ("metadata", {"items": [[{"value": "bad\x00value"}]]}),
        ("metadata", {"bad\x00key": "value"}),
        ("metadata", {"items": [{"bad\x00key": "value"}]}),
    ],
)
async def test_null_characters_are_rejected(api, body, field, value):
    response = await api.post(
        "/api/v1/payments",
        json={**body, field: value},
        headers={"Idempotency-Key": "null-character"},
    )
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", field]
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_valid_nested_metadata_is_preserved(api, body):
    data = {
        **body,
        "description": "Заказ\n123\t" + r"\u0000",
        "metadata": {
            "items": [{"label": "Товар", "literal": r"\u0000", "values": [None, True, 2, 1.5]}],
            "empty": {},
        },
    }
    response = await api.post(
        "/api/v1/payments", json=data, headers={"Idempotency-Key": "valid-metadata"}
    )
    assert response.status_code == 202
    detail = await api.get(f"/api/v1/payments/{response.json()['payment_id']}")
    assert detail.status_code == 200
    assert detail.json()["description"] == data["description"]
    assert detail.json()["metadata"] == data["metadata"]


async def test_idempotency_header_is_required(api, body):
    assert (await api.post("/api/v1/payments", json=body)).status_code == 422
    response = await api.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "   "})
    assert response.status_code == 422
