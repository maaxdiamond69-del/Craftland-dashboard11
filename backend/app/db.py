import os
from datetime import datetime, timezone

from .bootstrap import load_env

load_env()
from sqlalchemy import (create_engine, String, Integer, BigInteger, DateTime, ForeignKey, Text, JSON,
                        UniqueConstraint, Index, Float)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./botdash.db")
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=10, max_overflow=20) \
    if not DATABASE_URL.startswith("sqlite") else create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def utcnow():
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Admin(Base):
    __tablename__ = "admins"
    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(64), unique=True)
    password_hash: Mapped[str] = mapped_column(String(256))


class Server(Base):
    __tablename__ = "servers"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)          # no duplicate servers
    hostname: Mapped[str | None] = mapped_column(String(200))
    os: Mapped[str | None] = mapped_column(String(100))
    last_seen: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    bots = relationship("Bot", back_populates="server")


class Bot(Base):
    __tablename__ = "bots"
    id: Mapped[str] = mapped_column(String(20), primary_key=True)         # internal Bot ID, e.g. bot_a1b2c3d4
    name: Mapped[str] = mapped_column(String(100), unique=True)
    telegram_id: Mapped[int | None] = mapped_column(BigInteger, unique=True)   # unique -> no double registration
    telegram_username: Mapped[str | None] = mapped_column(String(100))
    token_enc: Mapped[str | None] = mapped_column(Text)                   # Fernet-encrypted, never returned by API
    pairing_hash: Mapped[str | None] = mapped_column(String(64), index=True)   # sha256 of one-time pairing key
    agent_hash: Mapped[str | None] = mapped_column(String(64), unique=True)    # sha256 of per-agent key
    server_id: Mapped[int | None] = mapped_column(ForeignKey("servers.id"))
    last_heartbeat: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    app_status: Mapped[str | None] = mapped_column(String(30))
    pending: Mapped[int] = mapped_column(Integer, default=0)
    processing: Mapped[int] = mapped_column(Integer, default=0)
    completed: Mapped[int] = mapped_column(Integer, default=0)
    failed_maps: Mapped[int] = mapped_column(Integer, default=0)
    errors: Mapped[int] = mapped_column(Integer, default=0)
    was_online: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    server = relationship("Server", back_populates="bots")


class BotStat(Base):                       # history for charts
    __tablename__ = "bot_stats"
    id: Mapped[int] = mapped_column(primary_key=True)
    bot_id: Mapped[str] = mapped_column(ForeignKey("bots.id", ondelete="CASCADE"))
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    pending: Mapped[int] = mapped_column(Integer)
    processing: Mapped[int] = mapped_column(Integer)
    completed: Mapped[int] = mapped_column(Integer)
    failed_maps: Mapped[int] = mapped_column(Integer, default=0)
    done_delta: Mapped[int] = mapped_column(Integer, default=0)           # IDs finished since previous sample
    __table_args__ = (Index("ix_stats_bot_ts", "bot_id", "ts"),)


class BotLog(Base):
    __tablename__ = "bot_logs"
    id: Mapped[int] = mapped_column(primary_key=True)
    bot_id: Mapped[str] = mapped_column(ForeignKey("bots.id", ondelete="CASCADE"))
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    level: Mapped[str] = mapped_column(String(10))       # INFO WARNING ERROR COMMAND MESSAGE SYSTEM
    event: Mapped[str] = mapped_column(String(50))
    message: Mapped[str] = mapped_column(Text)
    extra: Mapped[dict | None] = mapped_column(JSON)
    __table_args__ = (Index("ix_logs_bot_ts", "bot_id", "ts"), Index("ix_logs_level_ts", "level", "ts"))


class AgentConnection(Base):
    __tablename__ = "agent_connections"
    id: Mapped[int] = mapped_column(primary_key=True)
    bot_id: Mapped[str] = mapped_column(ForeignKey("bots.id", ondelete="CASCADE"), index=True)
    server_id: Mapped[int | None] = mapped_column(ForeignKey("servers.id"))
    ip: Mapped[str | None] = mapped_column(String(64))
    agent_version: Mapped[str | None] = mapped_column(String(20))
    connected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    disconnected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class StockAccount(Base):
    __tablename__ = "stock_accounts"
    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    secret: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(20), default="available", index=True)  # available | assigned
    bot_id: Mapped[str | None] = mapped_column(ForeignKey("bots.id", ondelete="SET NULL"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    assigned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WithdrawalTask(Base):
    __tablename__ = "withdrawal_tasks"
    id: Mapped[int] = mapped_column(primary_key=True)
    batch_id: Mapped[str] = mapped_column(String(32), index=True)
    bot_id: Mapped[str] = mapped_column(ForeignKey("bots.id", ondelete="CASCADE"), index=True)
    uid: Mapped[str] = mapped_column(String(64))
    secret: Mapped[str] = mapped_column(Text)
    remove: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)  # pending | done
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    __table_args__ = (Index("ix_withdraw_bot_status", "bot_id", "status"),)


class BotFileSnapshot(Base):               # latest copy of each JSON file
    __tablename__ = "bot_file_snapshots"
    id: Mapped[int] = mapped_column(primary_key=True)
    bot_id: Mapped[str] = mapped_column(ForeignKey("bots.id", ondelete="CASCADE"))
    kind: Mapped[str] = mapped_column(String(10))        # pending | progress | done
    data: Mapped[dict] = mapped_column(JSON)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    __table_args__ = (UniqueConstraint("bot_id", "kind", name="uq_snapshot_bot_kind"),)


def init_db():
    Base.metadata.create_all(engine)
