from datetime import datetime
import os
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://chat:chat@postgres:5432/chat")


class Base(DeclarativeBase):
    pass


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    room_id: Mapped[str] = mapped_column(String(100), index=True)
    username: Mapped[str] = mapped_column(String(64))
    text: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


engine = create_async_engine(DATABASE_URL, future=True, echo=False)
SessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

class Reaction(Base):
    __tablename__ = "reactions"
    # Одна и та же пара (сообщение, ник, эмодзи) не может повториться.
    __table_args__ = (UniqueConstraint("message_id", "username", "emoji"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    message_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("messages.id", ondelete="CASCADE"),  # при удалении сообщения — строки реакций уходят
        index=True,
    )
    username: Mapped[str] = mapped_column(String(64))
    emoji: Mapped[str] = mapped_column(String(8))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class ReadCursor(Base):
    __tablename__ = "read_cursors"
    # Один курсор на пользователя в комнате — upsert будет по этой паре.
    __table_args__ = (UniqueConstraint("username", "room_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(64), index=True)
    room_id: Mapped[str] = mapped_column(String(100), index=True)
    last_read_message_id: Mapped[int] = mapped_column(Integer)  # «дочитал до» этого id сообщения
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


async def init_models() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)