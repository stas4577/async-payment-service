import asyncio
from uuid import uuid4

import aio_pika
import httpx
import pytest
import pytest_asyncio
import uvicorn
from fastapi import FastAPI, Request, Response
from sqlalchemy import text

from app import consumer
from app.config import settings
from app.database import engine
from app.main import app
from app.rabbit import declare_queues, make_broker


@pytest_asyncio.fixture
async def rabbit():
    async with make_broker() as publisher:
        await declare_queues(publisher)
        connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        async with connection:
            channel = await connection.channel()
            main = await channel.declare_queue("payments.new", passive=True)
            dlq = await channel.declare_queue("payments.dlq", passive=True)
            await main.purge()
            await dlq.purge()
            yield publisher, main, dlq


@pytest_asyncio.fixture(autouse=True)
async def clean_database(rabbit):
    # Run only against the isolated test Compose project: each test clears these tables.
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE outbox, payments"))
    yield


@pytest_asyncio.fixture
async def api():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://api",
        headers={"X-API-Key": settings.api_key.get_secret_value()},
    ) as client:
        yield client


@pytest.fixture
def body():
    return {
        "amount": "125.50",
        "currency": "RUB",
        "description": "Test payment",
        "metadata": {"order_id": "order-123"},
        "webhook_url": "http://127.0.0.1:8088/webhook",
    }


@pytest_asyncio.fixture
async def payment_id(api, body):
    response = await api.post(
        "/api/v1/payments", json=body, headers={"Idempotency-Key": str(uuid4())}
    )
    assert response.status_code == 202, response.text
    return response.json()["payment_id"]


@pytest_asyncio.fixture
async def worker(rabbit):
    await consumer.broker.start()
    yield
    await consumer.broker.stop()


@pytest_asyncio.fixture
async def webhook():
    receiver = FastAPI()
    calls = []
    fail_first = {"count": 0}

    @receiver.post("/webhook")
    async def receive(request: Request):
        calls.append({"body": await request.json(), "headers": dict(request.headers)})
        return Response(status_code=503 if len(calls) <= fail_first["count"] else 204)

    server = uvicorn.Server(
        uvicorn.Config(receiver, host="127.0.0.1", port=8088, log_level="error")
    )
    task = asyncio.create_task(server.serve())
    async with asyncio.timeout(5):
        while not server.started:
            if task.done():
                await task
            await asyncio.sleep(0.01)
    yield calls, fail_first
    server.should_exit = True
    await task


@pytest_asyncio.fixture(scope="session", autouse=True)
async def close_resources():
    yield
    await consumer.http_client.aclose()
    await engine.dispose()
