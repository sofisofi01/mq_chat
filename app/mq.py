"""Подключение к RabbitMQ для web-процесса: topic-exchange, очереди и публикация.

Воркер поднимает те же сущности у себя; имена exchange/очередей и ключи задаются здесь и через env."""

import asyncio
import json
import os

import aio_pika

# URL брокера в Docker по умолчанию — хост `rabbitmq` из compose.
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
# Один topic-exchange: маршрутизация по routing key.
MQ_EXCHANGE = os.getenv("MQ_EXCHANGE", "chat.events")
# Входящая — читает только worker; «после БД» — читает фоновая задача в main.py.
MQ_QUEUE_INCOMING = os.getenv("MQ_QUEUE_INCOMING", "chat.messages.incoming")
MQ_QUEUE_PERSISTED = os.getenv("MQ_QUEUE_PERSISTED", "chat.messages.persisted")
MQ_ROUTING_KEY_CREATED = os.getenv("MQ_ROUTING_KEY_CREATED", "chat.message.created")
MQ_ROUTING_KEY_PERSISTED = os.getenv("MQ_ROUTING_KEY_PERSISTED", "chat.message.persisted")

# Реакции на сообщения.
MQ_QUEUE_REACTIONS_INCOMING = os.getenv("MQ_QUEUE_REACTIONS_INCOMING", "chat.reactions.incoming")
MQ_QUEUE_REACTIONS_PERSISTED = os.getenv("MQ_QUEUE_REACTIONS_PERSISTED", "chat.reactions.persisted")
MQ_ROUTING_KEY_REACTION_CREATED = os.getenv("MQ_ROUTING_KEY_REACTION_CREATED", "chat.reaction.created")
MQ_ROUTING_KEY_REACTION_PERSISTED = os.getenv("MQ_ROUTING_KEY_REACTION_PERSISTED", "chat.reaction.persisted")


class MQ:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.exchange = None

    async def connect(self):
        # Несколько попыток: web может стартовать раньше готовности RabbitMQ в compose.
        last_error = None
        for _ in range(20):
            try:
                self.connection = await aio_pika.connect_robust(RABBITMQ_URL)
                break
            except Exception as exc:
                last_error = exc
                await asyncio.sleep(1)
        if self.connection is None:
            raise RuntimeError(f"Cannot connect to RabbitMQ: {last_error}")
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            MQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
        )
        # declare_queue идемпотентен: те же имена, что у worker и у consume в main.
        for name in (
            MQ_QUEUE_INCOMING, MQ_QUEUE_PERSISTED,
            MQ_QUEUE_REACTIONS_INCOMING, MQ_QUEUE_REACTIONS_PERSISTED,
        ):
            await self.channel.declare_queue(name, durable=True)

        bindings = [
            (MQ_QUEUE_INCOMING,             MQ_ROUTING_KEY_CREATED),
            (MQ_QUEUE_PERSISTED,            MQ_ROUTING_KEY_PERSISTED),
            (MQ_QUEUE_REACTIONS_INCOMING,   MQ_ROUTING_KEY_REACTION_CREATED),
            (MQ_QUEUE_REACTIONS_PERSISTED,  MQ_ROUTING_KEY_REACTION_PERSISTED),
        ]
        for queue_name, routing_key in bindings:
            q = await self.channel.get_queue(queue_name)
            await q.bind(self.exchange, routing_key=routing_key)

    async def publish(self, routing_key: str, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        # PERSISTENT — просим брокер не терять сообщение при перезапуске (наряду с durable).
        msg = aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
        await self.exchange.publish(msg, routing_key=routing_key)

    async def close(self):
        if self.connection:
            await self.connection.close()


mq = MQ()