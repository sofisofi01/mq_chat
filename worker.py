import asyncio

import aio_pika

from app.db import init_models
from app.mq import (
    MQ_EXCHANGE,
    MQ_QUEUE_INCOMING,
    MQ_QUEUE_REACTIONS_INCOMING,
    MQ_ROUTING_KEY_CREATED,
    MQ_ROUTING_KEY_REACTION_CREATED,
    RABBITMQ_URL,
)
from workers.messages import handle_message
from workers.reactions import handle_reaction


async def consume(queue_name, routing_key, handler, channel, exchange):
    """Очередь + bind + бесконечный цикл: каждое сообщение — await handler(...)."""
    queue = await channel.declare_queue(queue_name, durable=True)
    await queue.bind(exchange, routing_key=routing_key)
    async with queue.iterator() as iterator:
        async for incoming in iterator:
            await handler(incoming, exchange)


async def run_worker():
    await init_models()  # таблицы messages / reactions / read_cursors при необходимости
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    exchange = await channel.declare_exchange(
        MQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )

    # Две «зелёные нити» в одном процессе: не блокируют друг друга на ожидании I/O.
    await asyncio.gather(
        consume(MQ_QUEUE_INCOMING, MQ_ROUTING_KEY_CREATED, handle_message, channel, exchange),
        consume(
            MQ_QUEUE_REACTIONS_INCOMING,
            MQ_ROUTING_KEY_REACTION_CREATED,
            handle_reaction,
            channel,
            exchange,
        ),
    )


if __name__ == "__main__":
    asyncio.run(run_worker())