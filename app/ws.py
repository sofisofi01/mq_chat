from collections import defaultdict
from fastapi import WebSocket


class WSManager:
    def __init__(self) -> None:
        # room_id -> набор активных WebSocket-подключений.
        self.rooms: dict[str, set[WebSocket]] = defaultdict(set)

    async def connect(self, room_id: str, ws: WebSocket) -> None:
        # При подключении клиент должен быть принят и добавлен в комнату.
        await ws.accept()
        self.rooms[room_id].add(ws)

    def disconnect(self, room_id: str, ws: WebSocket) -> None:
        # Удаляем клиента и чистим пустую комнату.
        if room_id in self.rooms and ws in self.rooms[room_id]:
            self.rooms[room_id].remove(ws)
            if not self.rooms[room_id]:
                del self.rooms[room_id]

    async def broadcast(self, room_id: str, html_fragment: str) -> None:
        # Рассылка HTML-фрагмента всем участникам комнаты.
        dead: list[WebSocket] = []
        for ws in self.rooms.get(room_id, set()):
            try:
                await ws.send_text(html_fragment)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(room_id, ws)


manager = WSManager()

class SignalingManager:
    """Отдельные WebSocket только для JSON WebRTC (не смешивать с HTML чата)."""

    def __init__(self) -> None:
        self.rooms: dict[str, set[WebSocket]] = defaultdict(set)

    async def connect(self, room_id: str, ws: WebSocket) -> None:
        await ws.accept()
        self.rooms[room_id].add(ws)

    def disconnect(self, room_id: str, ws: WebSocket) -> None:
        if room_id in self.rooms and ws in self.rooms[room_id]:
            self.rooms[room_id].remove(ws)
            if not self.rooms[room_id]:
                del self.rooms[room_id]

    async def relay(self, room_id: str, sender: WebSocket, text: str) -> None:
        """Переслать сырую строку всем другим сокетам в комнате (1:1 — одному пиру)."""
        dead: list[WebSocket] = []
        for peer in self.rooms.get(room_id, set()):
            if peer is sender:
                continue
            try:
                await peer.send_text(text)
            except Exception:
                dead.append(peer)
        for w in dead:
            self.disconnect(room_id, w)


signal_manager = SignalingManager()