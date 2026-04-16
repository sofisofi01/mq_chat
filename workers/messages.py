"""Обработчик chat.message.created: сохранить сообщение в БД и опубликовать chat.message.persisted."""

import json

import aio_pika
from sqlalchemy import insert

from app.db import Message, SessionLocal
from app.mq import MQ_ROUTING_KEY_PERSISTED


async def handle_message(incoming, exchange):
    """Сохранить сообщение в БД, опубликовать chat.message.persisted."""
    async with incoming.process(requeue=True):
        payload = json.loads(incoming.body.decode("utf-8"))

        async with SessionLocal() as session:
            stmt = insert(Message).values(
                room_id=payload["room_id"],
                username=payload["username"],
                text=payload["text"],
            ).returning(Message.id, Message.created_at)
            row = (await session.execute(stmt)).one()
            await session.commit()

        persisted = {
            "id": row.id,
            "room_id": payload["room_id"],
            "username": payload["username"],
            "text": payload["text"],
            "created_at": row.created_at.isoformat(),
        }
        msg = aio_pika.Message(
            body=json.dumps(persisted).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await exchange.publish(msg, routing_key=MQ_ROUTING_KEY_PERSISTED)