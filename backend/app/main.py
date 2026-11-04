import asyncio, csv, io, json, os, re, secrets, threading, uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import anyio
import requests
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import func, select, delete, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import security as sec
from .db import (Admin, AgentConnection, Bot, BotFileSnapshot, BotLog, BotStat, Server, SessionLocal,
                 StockAccount, WithdrawalTask, init_db, utcnow)

OFFLINE_AFTER = int(os.environ.get("OFFLINE_AFTER_SECONDS", "90"))
REFILL_BATCH = int(os.environ.get("REFILL_BATCH", "5000"))
_stock_lock = threading.Lock()
LOG_RETENTION_DAYS = int(os.environ.get("LOG_RETENTION_DAYS", "30"))
LEVELS = {"INFO", "WARNING", "ERROR", "COMMAND", "MESSAGE", "SYSTEM"}
FILE_KINDS = {"pending", "progress", "done"}
TOKEN_RE = re.compile(r"^\d{6,12}:[A-Za-z0-9_-]{30,}$")

app = FastAPI(title="Bot Monitoring Dashboard", docs_url=None, redoc_url=None)


# ------------------------------------------------------------------ websocket hub
class Hub:
    def __init__(self):
        self.clients: set[WebSocket] = set()

    async def broadcast(self, msg: dict):
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


hub = Hub()


def push(msg: dict):
    """Broadcast from sync (threadpool) endpoints."""
    try:
        anyio.from_thread.run(hub.broadcast, msg)
    except Exception:
        pass


# ------------------------------------------------------------------ deps
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def admin_required(request: Request):
    user = sec.read_session(request.cookies.get("session"))
    if not user:
        raise HTTPException(401, "Login required")
    return user


def agent_bot(request: Request, db: Session = Depends(get_db)) -> Bot:
    key = request.headers.get("X-Agent-Key", "")
    bot = db.scalar(select(Bot).where(Bot.agent_hash == sec.sha(key))) if key else None
    if not bot:
        raise HTTPException(401, "Invalid agent key")
    return bot


def iso(d):
    return d.astimezone(timezone.utc).isoformat() if d else None


def aware(d):
    return d if d is None or d.tzinfo else d.replace(tzinfo=timezone.utc)


def heartbeat_fresh(b: Bot) -> bool:
    hb = aware(b.last_heartbeat)
    return bool(hb and (utcnow() - hb).total_seconds() <= OFFLINE_AFTER)


def is_online(b: Bot) -> bool:
    # Monitor heartbeat alone is not enough. The bot script must be running.
    return heartbeat_fresh(b) and (b.app_status or "") == "running"


def bot_dict(b: Bot) -> dict:          # NOTE: never includes token_enc / key hashes
    return {"id": b.id, "name": b.name, "telegram_username": b.telegram_username,
            "online": is_online(b), "paired": b.agent_hash is not None,
            "server": b.server.name if b.server else None, "app_status": b.app_status,
            "pending": b.pending, "processing": b.processing, "completed": b.completed,
            "failed_maps": b.failed_maps, "errors": b.errors,
            "last_heartbeat": iso(b.last_heartbeat), "created_at": iso(b.created_at)}


def log_dict(l: BotLog, bot_name: str | None = None) -> dict:
    return {"id": l.id, "bot_id": l.bot_id, "bot": bot_name, "ts": iso(l.ts), "level": l.level,
            "event": l.event, "message": l.message}


def add_log(db: Session, bot: Bot, level: str, event: str, message: str, ts=None):
    level = level if level in LEVELS else "INFO"
    row = BotLog(bot_id=bot.id, level=level, event=event[:50], message=sec.scrub(message)[:2000], ts=ts or utcnow())
    db.add(row)
    if level == "ERROR":
        bot.errors = (bot.errors or 0) + 1
    return row


# ------------------------------------------------------------------ auth
class Login(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=200)


@app.post("/api/auth/login", dependencies=[Depends(sec.rate_limit("login", 10))])
def login(data: Login, response: Response, db: Session = Depends(get_db)):
    a = db.scalar(select(Admin).where(Admin.username == data.username))
    if not a or not sec.verify_password(data.password, a.password_hash):
        raise HTTPException(401, "Wrong username or password")
    response.set_cookie("session", sec.make_session(a.username), httponly=True, samesite="strict",
                        secure=sec.COOKIE_SECURE, max_age=sec.SESSION_HOURS * 3600)
    return {"ok": True, "username": a.username}


@app.post("/api/auth/logout")
def logout(response: Response):
    response.delete_cookie("session")
    return {"ok": True}


@app.get("/api/auth/me")
def me(user: str = Depends(admin_required)):
    return {"username": user}


class PwChange(BaseModel):
    current: str = Field(max_length=200)
    new: str = Field(min_length=10, max_length=200)


@app.post("/api/auth/password", dependencies=[Depends(sec.rate_limit("pw", 5))])
def change_password(data: PwChange, user: str = Depends(admin_required), db: Session = Depends(get_db)):
    a = db.scalar(select(Admin).where(Admin.username == user))
    if not a or not sec.verify_password(data.current, a.password_hash):
        raise HTTPException(400, "Current password is wrong")
    a.password_hash = sec.hash_password(data.new)
    db.commit()
    return {"ok": True}


# ------------------------------------------------------------------ bots
class NewBot(BaseModel):
    name: Optional[str] = Field(default=None, max_length=100)
    telegram_token: Optional[str] = Field(default=None, max_length=200)   # optional


def new_pairing(bot: Bot) -> str:
    key = sec.new_secret("pair")
    bot.pairing_hash = sec.sha(key)
    return key


@app.post("/api/bots", dependencies=[Depends(sec.rate_limit("addbot", 20))])
def add_bot(data: NewBot, _: str = Depends(admin_required), db: Session = Depends(get_db)):
    name, tg_id, tg_user, tok_enc = (data.name or "").strip() or None, None, None, None
    if data.telegram_token:
        token = data.telegram_token.strip()
        if not TOKEN_RE.match(token):
            raise HTTPException(422, "That does not look like a Telegram bot token")
        try:
            r = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=10)
            j = r.json()
        except Exception:
            raise HTTPException(502, "Could not reach Telegram to validate the token")
        if not j.get("ok"):
            raise HTTPException(400, "Telegram rejected this token")
        res = j["result"]
        tg_id, tg_user = res["id"], res.get("username")
        name = name or res.get("first_name") or tg_user
        tok_enc = sec.encrypt_token(token)                    # encrypted at rest; token itself is dropped here
        if db.scalar(select(Bot).where(Bot.telegram_id == tg_id)):
            raise HTTPException(409, "This Telegram bot is already registered")
    if not name:
        raise HTTPException(422, "Give the bot a name or paste a Telegram token")
    if db.scalar(select(Bot).where(Bot.name == name)):
        raise HTTPException(409, "A bot with this name already exists")
    bot = Bot(id="bot_" + uuid.uuid4().hex[:8], name=name, telegram_id=tg_id, telegram_username=tg_user, token_enc=tok_enc)
    pairing = new_pairing(bot)
    db.add(bot)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(409, "Bot already registered")
    push({"type": "bot_added", "bot": bot_dict(bot)})
    return {"bot": bot_dict(bot), "pairing_key": pairing}     # pairing key is shown ONCE


@app.post("/api/bots/{bot_id}/pairing")
def regen_pairing(bot_id: str, _: str = Depends(admin_required), db: Session = Depends(get_db)):
    bot = db.get(Bot, bot_id) or _404()
    key = new_pairing(bot)
    bot.agent_hash = None                                     # old agent key stops working
    db.commit()
    return {"pairing_key": key}


@app.delete("/api/bots/{bot_id}")
def delete_bot(bot_id: str, _: str = Depends(admin_required), db: Session = Depends(get_db)):
    bot = db.get(Bot, bot_id) or _404()
    for m in (BotLog, BotStat, BotFileSnapshot, AgentConnection):
        db.execute(delete(m).where(m.bot_id == bot.id))
    db.delete(bot)
    db.commit()
    push({"type": "bot_removed", "id": bot_id})
    return {"ok": True}


def _404():
    raise HTTPException(404, "Not found")


@app.get("/api/bots")
def list_bots(q: str = "", status: str = "", server: str = "", _: str = Depends(admin_required), db: Session = Depends(get_db)):
    bots = db.scalars(select(Bot).order_by(Bot.name)).all()
    out = [bot_dict(b) for b in bots]
    if q:
        ql = q.lower()
        out = [b for b in out if ql in (b["name"] or "").lower() or ql in (b["telegram_username"] or "").lower() or ql in b["id"]]
    if status in ("online", "offline"):
        out = [b for b in out if b["online"] == (status == "online")]
    if server:
        out = [b for b in out if b["server"] == server]
    return out


@app.get("/api/bots/{bot_id}")
def bot_detail(bot_id: str, _: str = Depends(admin_required), db: Session = Depends(get_db)):
    b = db.get(Bot, bot_id) or _404()
    logs = db.scalars(select(BotLog).where(BotLog.bot_id == b.id).order_by(BotLog.id.desc()).limit(100)).all()
    errs = db.scalars(select(BotLog).where(BotLog.bot_id == b.id, BotLog.level == "ERROR").order_by(BotLog.id.desc()).limit(50)).all()
    return {**bot_dict(b), "recent_logs": [log_dict(l, b.name) for l in logs], "error_history": [log_dict(l, b.name) for l in errs]}


# ------------------------------------------------------------------ JSON file viewer
def file_rows(kind: str, data: dict) -> list[dict]:
    if kind == "progress":
        rows = []
        for uid, st in data.items():
            st = st if isinstance(st, dict) else {}
            rows.append({"uid": uid, "completed_maps": len(st.get("completed_maps", [])),
                         "failed_maps": len(st.get("failed_maps", [])), "retry_done": bool(st.get("retry_done")),
                         "raw": st})
        return rows
    return [{"uid": str(uid), "password": value} for uid, value in data.items()]


@app.get("/api/bots/{bot_id}/files/{kind}")
def view_file(bot_id: str, kind: str, q: str = "", page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=500),
              _: str = Depends(admin_required), db: Session = Depends(get_db)):
    if kind not in FILE_KINDS:
        raise HTTPException(404, "Unknown file")
    snap = db.scalar(select(BotFileSnapshot).where(BotFileSnapshot.bot_id == bot_id, BotFileSnapshot.kind == kind))
    rows = file_rows(kind, snap.data if snap else {})
    if q:
        rows = [r for r in rows if q.lower() in r["uid"].lower()]
    total = len(rows)
    start = (page - 1) * page_size
    return {"kind": kind, "total": total, "page": page, "page_size": page_size,
            "updated_at": iso(snap.updated_at) if snap else None, "rows": rows[start:start + page_size]}


@app.get("/api/bots/{bot_id}/files/{kind}/export")
def export_file(bot_id: str, kind: str, format: str = "json", _: str = Depends(admin_required), db: Session = Depends(get_db)):
    if kind not in FILE_KINDS:
        raise HTTPException(404, "Unknown file")
    snap = db.scalar(select(BotFileSnapshot).where(BotFileSnapshot.bot_id == bot_id, BotFileSnapshot.kind == kind))
    rows = file_rows(kind, snap.data if snap else {})
    name = f"{bot_id}_{kind}"
    if format == "csv":
        buf = io.StringIO()
        keys = [k for k in (rows[0].keys() if rows else ["uid"]) if k != "raw"]
        w = csv.DictWriter(buf, fieldnames=keys, extrasaction="ignore")
        w.writeheader(); w.writerows(rows)
        body, mt, ext = buf.getvalue(), "text/csv", "csv"
    else:
        body, mt, ext = json.dumps([{k: v for k, v in r.items()} for r in rows], indent=2), "application/json", "json"
    return StreamingResponse(iter([body]), media_type=mt, headers={"Content-Disposition": f'attachment; filename="{name}.{ext}"'})


# ------------------------------------------------------------------ logs
@app.get("/api/bots/{bot_id}/screen")
def bot_screen(bot_id: str, _: str = Depends(admin_required), db: Session = Depends(get_db)):
    if not db.get(Bot, bot_id):
        _404()
    snap = db.scalar(select(BotFileSnapshot).where(BotFileSnapshot.bot_id == bot_id, BotFileSnapshot.kind == "screen"))
    text = (snap.data or {}).get("text", "") if snap else ""
    return {"text": text, "updated_at": iso(snap.updated_at) if snap else None}


@app.get("/api/logs")
def logs(bot_id: str = "", level: str = "", event: str = "", date_from: str = "", date_to: str = "", q: str = "",
         limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0),
         _: str = Depends(admin_required), db: Session = Depends(get_db)):
    stmt = select(BotLog, Bot.name).join(Bot, Bot.id == BotLog.bot_id)
    if bot_id: stmt = stmt.where(BotLog.bot_id == bot_id)
    if level:
        if level.upper() not in LEVELS: raise HTTPException(422, "bad level")
        stmt = stmt.where(BotLog.level == level.upper())
    if event: stmt = stmt.where(BotLog.event == event)
    if q: stmt = stmt.where(BotLog.message.ilike(f"%{q}%"))
    try:
        if date_from: stmt = stmt.where(BotLog.ts >= datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc))
        if date_to: stmt = stmt.where(BotLog.ts < datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc) + timedelta(days=1))
    except ValueError:
        raise HTTPException(422, "bad date (use YYYY-MM-DD)")
    rows = db.execute(stmt.order_by(BotLog.id.desc()).limit(limit).offset(offset)).all()
    events = [e for (e,) in db.execute(select(BotLog.event).distinct().limit(100)).all()]
    return {"logs": [log_dict(l, n) for l, n in rows], "events": sorted(events)}


# ------------------------------------------------------------------ overview / servers / analytics
def today_start():
    n = utcnow()
    return n.replace(hour=0, minute=0, second=0, microsecond=0)


@app.get("/api/overview")
def overview(_: str = Depends(admin_required), db: Session = Depends(get_db)):
    bots = db.scalars(select(Bot)).all()
    online = sum(1 for b in bots if is_online(b))
    processed_today = db.scalar(select(func.coalesce(func.sum(BotStat.done_delta), 0)).where(BotStat.ts >= today_start()))
    return {"total_bots": len(bots), "online": online, "offline": len(bots) - online,
            "pending": sum(b.pending for b in bots), "processing": sum(b.processing for b in bots),
            "completed": sum(b.completed for b in bots), "errors": sum(b.errors for b in bots),
            "processed_today": int(processed_today or 0)}


@app.get("/api/servers")
def servers(_: str = Depends(admin_required), db: Session = Depends(get_db)):
    out = []
    for s in db.scalars(select(Server).order_by(Server.name)).all():
        bs = db.scalars(select(Bot).where(Bot.server_id == s.id)).all()
        out.append({"id": s.id, "name": s.name, "hostname": s.hostname, "os": s.os, "last_seen": iso(s.last_seen),
                    "bots": len(bs), "online_bots": sum(1 for b in bs if is_online(b)),
                    "pending": sum(b.pending for b in bs), "completed": sum(b.completed for b in bs)})
    return out


@app.get("/api/analytics/daily")
def analytics(days: int = Query(14, ge=1, le=90), bot_id: str = "", _: str = Depends(admin_required), db: Session = Depends(get_db)):
    since = today_start() - timedelta(days=days - 1)
    stmt = select(BotStat.ts, BotStat.done_delta, BotStat.failed_maps).where(BotStat.ts >= since)
    if bot_id: stmt = stmt.where(BotStat.bot_id == bot_id)
    buckets = {(since + timedelta(days=i)).date().isoformat(): 0 for i in range(days)}
    for ts, delta, _f in db.execute(stmt).all():
        k = aware(ts).date().isoformat()
        if k in buckets: buckets[k] += delta or 0
    errs = {k: 0 for k in buckets}
    es = select(BotLog.ts).where(BotLog.level == "ERROR", BotLog.ts >= since)
    if bot_id: es = es.where(BotLog.bot_id == bot_id)
    for (ts,) in db.execute(es).all():
        k = aware(ts).date().isoformat()
        if k in errs: errs[k] += 1
    return {"days": [{"date": d, "processed": buckets[d], "errors": errs[d]} for d in buckets]}


# ------------------------------------------------------------------ agent API
class Register(BaseModel):
    pairing_key: str = Field(min_length=10, max_length=200)
    server_name: str = Field(min_length=1, max_length=100)
    hostname: Optional[str] = Field(default=None, max_length=200)
    os: Optional[str] = Field(default=None, max_length=100)
    agent_version: Optional[str] = Field(default=None, max_length=20)


@app.post("/api/agent/register", dependencies=[Depends(sec.rate_limit("register", 10))])
def agent_register(data: Register, request: Request, db: Session = Depends(get_db)):
    bot = db.scalar(select(Bot).where(Bot.pairing_hash == sec.sha(data.pairing_key)))
    if not bot or bot.agent_hash:                    # unknown key, or already used (one-time)
        raise HTTPException(403, "Invalid or already used pairing key")
    server = db.scalar(select(Server).where(Server.name == data.server_name))
    if not server:
        server = Server(name=data.server_name)
        db.add(server)
        db.flush()
    server.hostname, server.os, server.last_seen = data.hostname, data.os, utcnow()
    agent_key = sec.new_secret("agent")
    bot.agent_hash, bot.pairing_hash, bot.server_id = sec.sha(agent_key), None, server.id
    bot.last_heartbeat = utcnow()
    db.add(AgentConnection(bot_id=bot.id, server_id=server.id, ip=request.client.host if request.client else None,
                           agent_version=data.agent_version))
    add_log(db, bot, "SYSTEM", "agent_paired", f"agent paired from server '{server.name}'")
    db.commit()
    push({"type": "bot_update", "bot": bot_dict(bot)})
    return {"agent_key": agent_key, "bot_id": bot.id}


class Stats(BaseModel):
    pending: int = Field(ge=0, le=10_000_000)
    processing: int = Field(ge=0, le=10_000_000)
    completed: int = Field(ge=0, le=10_000_000)
    failed_maps: int = Field(default=0, ge=0, le=10_000_000)
    app_status: Optional[str] = Field(default=None, max_length=30)


class LogIn(BaseModel):
    ts: Optional[float] = None
    level: str = Field(max_length=10)
    event: str = Field(max_length=50)
    message: str = Field(max_length=2000)


class Report(BaseModel):
    stats: Stats
    files: dict = {}
    logs: list[LogIn] = Field(default=[], max_length=1000)
    process: dict = {}
    screen: Optional[str] = Field(default=None, max_length=100000)


def touch(db: Session, bot: Bot, app_status: Optional[str]):
    now = utcnow()
    if app_status:
        bot.app_status = app_status[:30]
    bot.last_heartbeat = now
    if bot.server:
        bot.server.last_seen = now
    online = is_online(bot)
    was = bool(bot.was_online)
    bot.was_online = 1 if online else 0
    if online and not was:
        add_log(db, bot, "SYSTEM", "online", "bot script is running")
    elif was and not online:
        add_log(db, bot, "WARNING", "offline", f"bot is offline ({bot.app_status or 'stopped'})")


@app.post("/api/agent/report", dependencies=[Depends(sec.rate_limit("report", 300))])
def agent_report(data: Report, bot: Bot = Depends(agent_bot), db: Session = Depends(get_db)):
    bot = db.merge(bot)
    s = data.stats
    delta = max(0, s.completed - bot.completed) if bot.completed is not None and (bot.pending or bot.processing or bot.completed) else 0
    changed = (s.pending, s.processing, s.completed, s.failed_maps) != (bot.pending, bot.processing, bot.completed, bot.failed_maps)
    bot.pending, bot.processing, bot.completed, bot.failed_maps = s.pending, s.processing, s.completed, s.failed_maps
    last = db.scalar(select(BotStat).where(BotStat.bot_id == bot.id).order_by(BotStat.id.desc()).limit(1))
    if changed or not last or (utcnow() - aware(last.ts)).total_seconds() > 300:
        db.add(BotStat(bot_id=bot.id, pending=s.pending, processing=s.processing, completed=s.completed,
                       failed_maps=s.failed_maps, done_delta=delta))
    new_logs = []
    for l in data.logs:
        ts = datetime.fromtimestamp(min(max(l.ts or 0, 0), utcnow().timestamp()), timezone.utc) if l.ts else None
        new_logs.append(add_log(db, bot, l.level.upper(), l.event, l.message, ts))
    for kind, content in data.files.items():
        if kind not in FILE_KINDS or not isinstance(content, dict) or len(json.dumps(content)) > 20_000_000:
            continue
        snap = db.scalar(select(BotFileSnapshot).where(BotFileSnapshot.bot_id == bot.id, BotFileSnapshot.kind == kind))
        if snap:
            snap.data, snap.updated_at = content, utcnow()
        else:
            db.add(BotFileSnapshot(bot_id=bot.id, kind=kind, data=content))
    if data.screen:
        snap = db.scalar(select(BotFileSnapshot).where(BotFileSnapshot.bot_id == bot.id, BotFileSnapshot.kind == "screen"))
        payload = {"text": data.screen}
        if snap:
            snap.data, snap.updated_at = payload, utcnow()
        else:
            db.add(BotFileSnapshot(bot_id=bot.id, kind="screen", data=payload))
    touch(db, bot, s.app_status or data.process.get("app_status"))
    db.commit()
    push({"type": "bot_update", "bot": bot_dict(bot)})
    if data.screen:
        push({"type": "screen", "bot_id": bot.id, "bot": bot.name, "text": data.screen})
    for l in new_logs:
        push({"type": "log", "log": log_dict(l, bot.name)})
    return {"ok": True}


class Heartbeat(BaseModel):
    ts: Optional[float] = None
    app_status: Optional[str] = Field(default=None, max_length=30)


@app.post("/api/agent/heartbeat", dependencies=[Depends(sec.rate_limit("hb", 300))])
def agent_heartbeat(data: Heartbeat, bot: Bot = Depends(agent_bot), db: Session = Depends(get_db)):
    bot = db.merge(bot)
    touch(db, bot, data.app_status)
    db.commit()
    push({"type": "bot_update", "bot": bot_dict(bot)})
    return {"ok": True}


# ------------------------------------------------------------------ central stock
class StockIn(BaseModel):
    text: str = Field(min_length=2, max_length=30_000_000)


class RefillIn(BaseModel):
    pending: int = Field(ge=0, le=10_000_000)


class ReleaseIn(BaseModel):
    uids: list[str] = Field(default=[], max_length=5000)


def parse_stock(text: str) -> dict[str, str]:
    text = text.strip()
    out: dict[str, str] = {}
    if text.startswith("{") or text.startswith("["):
        raw = json.loads(text)
        if isinstance(raw, dict):
            for uid, pwd in raw.items():
                if str(uid).strip() and pwd is not None and str(pwd) != "":
                    out[str(uid).strip()] = str(pwd)
        elif isinstance(raw, list):
            for item in raw:
                if isinstance(item, dict):
                    uid = item.get("uid", item.get("id"))
                    pwd = item.get("password", item.get("pass"))
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    uid, pwd = item[0], item[1]
                else:
                    continue
                if uid is not None and pwd is not None and str(pwd) != "":
                    out[str(uid).strip()] = str(pwd)
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" in line:
            uid, pwd = line.split(":", 1)
        else:
            parts = line.split()
            if len(parts) < 2:
                continue
            uid, pwd = parts[0], parts[1]
        uid, pwd = uid.strip(), pwd.strip()
        if uid and pwd:
            out[uid] = pwd
    return out


@app.get("/api/stock")
def stock_summary(_: str = Depends(admin_required), db: Session = Depends(get_db)):
    available = db.scalar(select(func.count()).select_from(StockAccount).where(StockAccount.status == "available")) or 0
    assigned = db.scalar(select(func.count()).select_from(StockAccount).where(StockAccount.status == "assigned")) or 0
    return {"available": int(available), "assigned": int(assigned), "batch": REFILL_BATCH}


@app.post("/api/stock", dependencies=[Depends(sec.rate_limit("stock", 20))])
def stock_add(data: StockIn, _: str = Depends(admin_required), db: Session = Depends(get_db)):
    try:
        incoming = parse_stock(data.text)
    except (json.JSONDecodeError, TypeError, ValueError):
        raise HTTPException(422, "Could not read that stock. Paste {\"uid\": \"password\"} JSON or uid:password lines.")
    if not incoming:
        raise HTTPException(422, "No accounts found in that paste")
    uids = list(incoming)
    existing = set()
    for i in range(0, len(uids), 800):
        chunk = uids[i:i + 800]
        existing.update(db.scalars(select(StockAccount.uid).where(StockAccount.uid.in_(chunk))).all())
    added = 0
    for uid, pwd in incoming.items():
        if uid in existing:
            continue
        db.add(StockAccount(uid=uid[:64], secret=pwd, status="available"))
        added += 1
    db.commit()
    return {"added": added, "skipped": len(incoming) - added}


@app.post("/api/agent/refill", dependencies=[Depends(sec.rate_limit("refill", 30))])
def agent_refill(data: RefillIn, bot: Bot = Depends(agent_bot), db: Session = Depends(get_db)):
    if data.pending != 0:
        return {"ok": True, "refilled": 0, "accounts": {}}
    with _stock_lock:
        bot = db.merge(bot)
        keep = (
            select(StockAccount.id)
            .where(StockAccount.status == "assigned", StockAccount.bot_id == bot.id)
            .order_by(StockAccount.id)
            .limit(REFILL_BATCH)
        )
        db.execute(
            update(StockAccount)
            .where(
                StockAccount.status == "assigned",
                StockAccount.bot_id == bot.id,
                StockAccount.id.not_in(keep),
            )
            .values(status="available", bot_id=None, assigned_at=None)
        )
        rows = db.scalars(
            select(StockAccount).where(StockAccount.id.in_(keep))
        ).all()
        if rows:
            accounts = {row.uid: row.secret for row in rows}
            add_log(db, bot, "SYSTEM", "refill", f"resent {len(accounts)} accounts already assigned to this bot")
            db.commit()
            return {"ok": True, "refilled": len(accounts), "accounts": accounts}
        ids = db.scalars(
            select(StockAccount.id).where(StockAccount.status == "available").order_by(StockAccount.id).limit(REFILL_BATCH)
        ).all()
        accounts = {}
        if ids:
            now = utcnow()
            db.execute(
                update(StockAccount)
                .where(StockAccount.id.in_(ids))
                .values(status="assigned", bot_id=bot.id, assigned_at=now)
            )
            rows = db.scalars(select(StockAccount).where(StockAccount.id.in_(ids))).all()
            accounts = {row.uid: row.secret for row in rows}
            add_log(db, bot, "SYSTEM", "refill", f"assigned {len(accounts)} accounts from stock")
        db.commit()
    return {"ok": True, "refilled": len(accounts), "accounts": accounts}


class WithdrawIn(BaseModel):
    count: int = Field(ge=1, le=65000)
    remove: bool = False


class WithdrawAck(BaseModel):
    uids: list[str] = Field(default=[], max_length=65000)


@app.post("/api/withdraw", dependencies=[Depends(sec.rate_limit("withdraw", 20))])
def withdraw(data: WithdrawIn, _: str = Depends(admin_required), db: Session = Depends(get_db)):
    pending = set(db.scalars(select(WithdrawalTask.uid).where(WithdrawalTask.status == "pending")).all())
    picked = []
    seen = set()
    bots = db.scalars(select(Bot).order_by(Bot.name)).all()
    for bot in bots:
        if len(picked) >= data.count:
            break
        snap = db.scalar(select(BotFileSnapshot).where(BotFileSnapshot.bot_id == bot.id, BotFileSnapshot.kind == "done"))
        raw = snap.data if snap and isinstance(snap.data, dict) else {}
        for uid in sorted(raw, key=str):
            if len(picked) >= data.count:
                break
            uid = str(uid)
            if uid in seen or uid in pending:
                continue
            pwd = raw[uid] if not isinstance(raw[uid], dict) else raw[uid]
            if pwd is None or str(pwd) in ("", "***"):
                continue
            seen.add(uid)
            picked.append((bot, uid, str(pwd)))
    batch = uuid.uuid4().hex[:12]
    if data.remove:
        counts: dict[str, int] = {}
        bots_by_id = {}
        for bot, uid, pwd in picked:
            db.add(WithdrawalTask(batch_id=batch, bot_id=bot.id, uid=uid[:64], secret=pwd, remove=1, status="pending"))
            counts[bot.id] = counts.get(bot.id, 0) + 1
            bots_by_id[bot.id] = bot
        for bot_id, n in counts.items():
            add_log(db, bots_by_id[bot_id], "SYSTEM", "withdraw", f"withdrawal queued, {n} ids")
        db.commit()
    by_bot: dict[str, int] = {}
    accounts = {}
    for bot, uid, pwd in picked:
        accounts[uid] = pwd
        by_bot[bot.name] = by_bot.get(bot.name, 0) + 1
    return {"taken": len(picked), "requested": data.count, "remove": data.remove,
            "accounts": accounts, "by_bot": [{"bot": name, "count": n} for name, n in by_bot.items()]}


@app.post("/api/agent/withdraw-pull", dependencies=[Depends(sec.rate_limit("withdraw", 120))])
def withdraw_pull(bot: Bot = Depends(agent_bot), db: Session = Depends(get_db)):
    bot = db.merge(bot)
    rows = db.scalars(select(WithdrawalTask).where(
        WithdrawalTask.bot_id == bot.id, WithdrawalTask.status == "pending", WithdrawalTask.remove == 1
    ).limit(65000)).all()
    return {"uids": [r.uid for r in rows]}


@app.post("/api/agent/withdraw-ack", dependencies=[Depends(sec.rate_limit("withdraw", 120))])
def withdraw_ack(data: WithdrawAck, bot: Bot = Depends(agent_bot), db: Session = Depends(get_db)):
    bot = db.merge(bot)
    wanted = {u[:64] for u in data.uids}
    if not wanted:
        return {"ok": True, "done": 0}
    done = 0
    uid_list = list(wanted)
    for i in range(0, len(uid_list), 800):
        chunk = uid_list[i:i + 800]
        rows = db.scalars(select(WithdrawalTask).where(
            WithdrawalTask.bot_id == bot.id, WithdrawalTask.status == "pending", WithdrawalTask.uid.in_(chunk))).all()
        for row in rows:
            row.status = "done"
            done += 1
    snap = db.scalar(select(BotFileSnapshot).where(BotFileSnapshot.bot_id == bot.id, BotFileSnapshot.kind == "done"))
    if snap and isinstance(snap.data, dict):
        snap.data = {k: v for k, v in snap.data.items() if str(k) not in wanted}
        snap.updated_at = utcnow()
    if done:
        add_log(db, bot, "INFO", "withdraw", f"removed {done} ids from done_accs.json")
    db.commit()
    return {"ok": True, "done": done}


@app.post("/api/agent/refill/release", dependencies=[Depends(sec.rate_limit("refill", 30))])
def agent_release(data: ReleaseIn, bot: Bot = Depends(agent_bot), db: Session = Depends(get_db)):
    bot = db.merge(bot)
    released = 0
    for i in range(0, len(data.uids), 800):
        chunk = [u[:64] for u in data.uids[i:i + 800]]
        rows = db.scalars(select(StockAccount).where(
            StockAccount.uid.in_(chunk), StockAccount.bot_id == bot.id, StockAccount.status == "assigned")).all()
        for row in rows:
            row.status = "available"
            row.bot_id = None
            row.assigned_at = None
            released += 1
    db.commit()
    return {"ok": True, "released": released}


# ------------------------------------------------------------------ websocket
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    if not sec.read_session(ws.cookies.get("session")):
        await ws.close(code=4401)
        return
    await ws.accept()
    hub.clients.add(ws)
    try:
        while True:
            await ws.receive_text()          # keep-alive pings from the client
    except WebSocketDisconnect:
        pass
    finally:
        hub.clients.discard(ws)


# ------------------------------------------------------------------ background jobs
def _offline_sweep():
    db = SessionLocal()
    try:
        changed = []
        for b in db.scalars(select(Bot).where(Bot.was_online == 1)).all():
            if not is_online(b):
                b.was_online = 0
                add_log(db, b, "WARNING", "offline", f"no heartbeat for {OFFLINE_AFTER}s - bot is offline")
                if b.id:
                    for c in db.scalars(select(AgentConnection).where(AgentConnection.bot_id == b.id, AgentConnection.disconnected_at.is_(None))).all():
                        c.disconnected_at = utcnow()
                changed.append(b)
        db.commit()
        return [bot_dict(b) for b in changed]
    finally:
        db.close()


def _prune():
    db = SessionLocal()
    try:
        db.execute(delete(BotLog).where(BotLog.ts < utcnow() - timedelta(days=LOG_RETENTION_DAYS)))
        db.commit()
    finally:
        db.close()


async def background():
    n = 0
    while True:
        await asyncio.sleep(15)
        try:
            for b in await anyio.to_thread.run_sync(_offline_sweep):
                await hub.broadcast({"type": "bot_update", "bot": b})
            n += 1
            if n % 240 == 0:
                await anyio.to_thread.run_sync(_prune)
        except Exception as e:
            print("background error:", e)


@app.on_event("startup")
async def startup():
    init_db()
    db = SessionLocal()
    try:
        if not db.scalar(select(Admin).limit(1)):
            pw = os.environ.get("ADMIN_PASSWORD") or secrets.token_urlsafe(12)
            db.add(Admin(username=os.environ.get("ADMIN_USER", "admin"), password_hash=sec.hash_password(pw)))
            db.commit()
            if not os.environ.get("ADMIN_PASSWORD"):
                print(f"\n=== First start: admin login  user={os.environ.get('ADMIN_USER', 'admin')}  password={pw}  (shown once) ===\n")
    finally:
        db.close()
    asyncio.create_task(background())


@app.middleware("http")
async def security_headers(request: Request, call_next):
    resp = await call_next(request)
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    return resp


_FRONT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "frontend")
if os.path.isdir(_FRONT):
    app.mount("/", StaticFiles(directory=_FRONT, html=True), name="frontend")
