import asyncio
import json
from html import escape
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, Form, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.db import Message, ReadCursor, SessionLocal, init_models
from app.mq import( mq,
    MQ_QUEUE_PERSISTED,
    MQ_QUEUE_REACTIONS_PERSISTED,
    MQ_ROUTING_KEY_CREATED,
    MQ_ROUTING_KEY_REACTION_CREATED,)
from app.ws import manager, signal_manager
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

templates = Jinja2Templates(directory="app/templates")
persisted_task: asyncio.Task | None = None


async def consume_persisted_events():
    queue = await mq.channel.get_queue(MQ_QUEUE_PERSISTED)
    async with queue.iterator() as iterator:
        async for message in iterator:
            async with message.process(requeue=True):
                data = json.loads(message.body.decode("utf-8"))
                html_fragment = (
                     '<div id="messages" hx-swap-oob="beforeend">'
                    f'<article id="msg-{data["id"]}" class="msg">'
                    "<header>"
                    f"<strong>{escape(str(data['username']))}</strong>"
                    f"<small>{escape(str(data['created_at']))}</small>"
                    f'<span id="status-{data["id"]}" class="msg-status" title="доставлено"></span>'
                    "</header>"
                    f"<p>{escape(str(data['text']))}</p>"
                    f'<div id="reactions-{data["id"]}" class="reactions"></div>'
                    "</article>"
                    "</div>"
                )
                await manager.broadcast(
                    data["room_id"],
                    html_fragment,
                )

async def consume_reaction_events():
    # Зеркало consume_persisted_events: другая очередь, другой формат JSON тела.
    queue = await mq.channel.get_queue(MQ_QUEUE_REACTIONS_PERSISTED)
    async with queue.iterator() as iterator:
        async for message in iterator:
            async with message.process(requeue=True):
                data = json.loads(message.body.decode("utf-8"))
                # Кнопки со счётчиком; escape(e) обязателен в onclick, если эмодзи в шаблоне.
                counts_html = "".join(
                    f'<button class="reaction" '
                    f'onclick="toggleReaction(\'{data["room_id"]}\',{data["message_id"]},\'{escape(e)}\')">'
                    f'{escape(e)} {c}</button>'
                    for e, c in data["counts"].items()
                )
                # innerHTML — заменить только блок реакций, не весь article.
                html_fragment = (
                    f'<div id="reactions-{data["message_id"]}" '
                    f'class="reactions" hx-swap-oob="innerHTML">'
                    + counts_html
                    + "</div>"
                )
                await manager.broadcast(data["room_id"], html_fragment)
@asynccontextmanager
async def lifespan(app: FastAPI):
    global persisted_task
    await init_models()
    await mq.connect()
    _bg_tasks = [
        asyncio.create_task(consume_persisted_events()),   # новые сообщения в чат
        asyncio.create_task(consume_reaction_events()),       # обновление счётчиков реакций
    ]
    yield
    for t in _bg_tasks:
        t.cancel()
    await mq.close()


app = FastAPI(title="Socket MQ Chat", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="room.html",
        context={"request": request, "room_id": "room-1"},
    )


@app.get("/rooms/{room_id}", response_class=HTMLResponse)
async def room_page(request: Request, room_id: str):
    return templates.TemplateResponse(
        request=request,
        name="room.html",
        context={"request": request, "room_id": room_id},
    )


@app.post("/rooms/{room_id}/messages")
async def send_message(room_id: str, username: str = Form(...), text: str = Form(...)):
    username = username.strip()
    text = text.strip()
    if not username or not text:
        raise HTTPException(status_code=400, detail="username/text cannot be empty")

    payload = {
        "room_id": room_id,
        "username": username,
        "text": text,
        "created_at": datetime.utcnow().isoformat(),
    }
    await mq.publish(MQ_ROUTING_KEY_CREATED, payload)
    return {"status": "queued"}


@app.websocket("/ws/{room_id}")
async def ws_room(ws: WebSocket, room_id: str):
    await manager.connect(room_id, ws)
    try:
        while True:
            raw = await ws.receive_text()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            username = str(data.get("username", "")).strip()
            text = str(data.get("text", "")).strip()
            if not username or not text:
                continue

            payload = {
                "room_id": room_id,
                "username": username,
                "text": text,
                "created_at": datetime.utcnow().isoformat(),
            }
            await mq.publish(MQ_ROUTING_KEY_CREATED, payload)
    except WebSocketDisconnect:
        manager.disconnect(room_id, ws)
    except Exception:
        manager.disconnect(room_id, ws)

@app.websocket("/ws/signal/{room_id}")
async def ws_signal(ws: WebSocket, room_id: str):
    """Только relay JSON между пирами в комнате; тело не парсится."""
    await signal_manager.connect(room_id, ws)
    try:
        while True:
            raw = await ws.receive_text()
            await signal_manager.relay(room_id, ws, raw)
    except WebSocketDisconnect:
        signal_manager.disconnect(room_id, ws)
    except Exception:
        signal_manager.disconnect(room_id, ws)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/rooms/{room_id}/messages/{message_id}/reactions")
async def add_reaction(room_id: str, message_id: int, username: str = Form(...), emoji: str = Form(...)):
    await mq.publish(MQ_ROUTING_KEY_REACTION_CREATED, {  # воркер: INSERT reaction
        "room_id": room_id,
        "message_id": message_id,
        "username": username.strip(),
        "emoji": emoji.strip(),
    })
    return {"status": "queued"}


@app.delete("/rooms/{room_id}/messages/{message_id}/reactions")
async def remove_reaction(room_id: str, message_id: int, username: str = Form(...), emoji: str = Form(...)):
    await mq.publish(MQ_ROUTING_KEY_REACTION_CREATED, {
        "room_id": room_id,
        "message_id": message_id,
        "username": username.strip(),
        "emoji": emoji.strip(),
        "delete": True,  # воркер выполнит DELETE вместо INSERT
    })
    return {"status": "queued"}

@app.post("/rooms/{room_id}/read")
async def mark_read(room_id: str, username: str = Form(...), message_id: int = Form(...)):
    """Курсор «прочитано»: БД + сразу broadcast ✓✓ (без очереди RabbitMQ)."""
    reader = username.strip()

    async with SessionLocal() as session:
        # Upsert курсора; не двигаем назад (см. where).
        stmt = pg_insert(ReadCursor).values(
            username=reader,
            room_id=room_id,
            last_read_message_id=message_id,
        ).on_conflict_do_update(
            index_elements=["username", "room_id"],
            set_={"last_read_message_id": message_id},
            where=(ReadCursor.last_read_message_id < message_id),
        )
        await session.execute(stmt)
        await session.commit()

        row = (
            await session.execute(
                select(Message.username).where(
                    Message.id == message_id,
                    Message.room_id == room_id,
                )
            )
        ).one_or_none()

    if row is None:
        return {"status": "ok"}
    if row[0] == reader:
        return {"status": "ok"}

    # Читатель ≠ автор: шлём oob, CSS нарисует ✓✓.
    html_fragment = (
        f'<span id="status-{message_id}" '
        f'class="msg-status read" hx-swap-oob="outerHTML" title="прочитано"></span>'
    )
    await manager.broadcast(room_id, html_fragment)
    return {"status": "ok"}