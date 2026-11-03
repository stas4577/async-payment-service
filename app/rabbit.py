from faststream.rabbit import Channel, RabbitBroker, RabbitQueue

from app.config import settings

new_payments = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-queue-type": "quorum",
        "x-delivery-limit": 2,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "payments.dlq",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)
dead_letters = RabbitQueue("payments.dlq", durable=True, arguments={"x-queue-type": "quorum"})


def make_broker() -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        graceful_timeout=45,
    )


async def declare_queues(broker: RabbitBroker) -> None:
    await broker.declare_queue(dead_letters)
    await broker.declare_queue(new_payments)
