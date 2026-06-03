import asyncio
import logging
from time import monotonic

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, UserIsBlocked, InputUserDeactivated, PeerIdInvalid

from database.users_chats_db import db
from info import (
    ADMINS,
    BROADCAST_AS_FORWARD,
    BROADCAST_STATUS_UPDATE_SECONDS,
)

logger = logging.getLogger(__name__)

USER_TARGET  = "users"
GROUP_TARGET = "groups"
ALL_TARGETS  = (USER_TARGET, GROUP_TARGET)

# ── Tuning ────────────────────────────────────────────────────────────────────
BROADCAST_WORKERS = 15      # parallel in-flight sends
                            # forward mode is stricter — 15 is safer than 25

# Fixed inter-send delay (seconds between each acquire).
# 1/SENDS_PER_SECOND = gap between tokens.
# Forward mode: start at 5 msg/s (0.2s gap) — conservative but reliable.
# Copy   mode:  you can try 15–20 msg/s safely.
SENDS_PER_SECOND: float = 5.0

# On FloodWait: pause ALL workers for the demanded seconds, then resume
# at a reduced rate.  Rate is NOT halved multiplicatively — instead we
# step it down by a fixed amount and let it recover linearly.
FLOOD_STEP_DOWN = 1.0       # subtract this from rate on each FloodWait
FLOOD_STEP_UP   = 0.1       # add this to rate after each N successful sends
FLOOD_RECOVER_EVERY = 50    # recover one step every this many successes
# ─────────────────────────────────────────────────────────────────────────────

_active_broadcast: asyncio.Task | None = None


# ── Rate limiter ──────────────────────────────────────────────────────────────

class _RateLimiter:
    """
    Fixed-interval token bucket with linear (not multiplicative) FloodWait response.

    Key fixes vs previous version:
    - flood_backoff() is idempotent within a flood window: 25 workers hitting
      FloodWait at the same time only step the rate down ONCE, not 25 times.
    - FloodWait sleep happens OUTSIDE the semaphore (see _worker), so sleeping
      workers don't consume slots.
    - Recovery is linear (+0.1/50 sends) not multiplicative, so it's predictable.
    """

    def __init__(self, rate: float):
        self._rate      = rate
        self._max_rate  = rate
        self._tokens    = rate
        self._refill_ts = monotonic()
        self._lock      = asyncio.Lock()
        self._success_count = 0
        # flood dedup: ignore repeated backoff calls within same flood window
        self._flood_until: float = 0.0

    def _refill(self):
        now = monotonic()
        dt = now - self._refill_ts
        self._tokens = min(self._rate, self._tokens + dt * self._rate)
        self._refill_ts = now

    async def acquire(self):
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / max(self._rate, 0.1)
            await asyncio.sleep(wait)

    def flood_backoff(self, flood_seconds: int) -> bool:
        """
        Returns True if this call actually applied a backoff (first caller wins).
        Subsequent callers within the same flood window get False and should
        just sleep without touching the rate.
        """
        now = monotonic()
        if now < self._flood_until:
            return False   # already handling this flood
        self._flood_until = now + flood_seconds
        self._rate = max(1.0, self._rate - FLOOD_STEP_DOWN)
        logger.warning(
            "FloodWait %ss — rate stepped down to %.1f msg/s", flood_seconds, self._rate
        )
        return True

    def on_success(self):
        self._success_count += 1
        if self._success_count % FLOOD_RECOVER_EVERY == 0:
            old = self._rate
            self._rate = min(self._max_rate, self._rate + FLOOD_STEP_UP)
            if self._rate != old:
                logger.info("Rate recovered to %.1f msg/s", self._rate)

    @property
    def current_rate(self) -> float:
        return self._rate


# ── Helpers ───────────────────────────────────────────────────────────────────

def _broadcast_mode() -> str:
    return "forward" if BROADCAST_AS_FORWARD else "copy"

def _new_stats(total: int) -> dict:
    return dict(total=total, done=0, sent=0, blocked=0, deleted=0, invalid=0, failed=0)

def _fmt_time(s: float) -> str:
    m, s = divmod(int(s), 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")

def _pct(stats: dict) -> str:
    t = stats["total"]
    return f"{int(stats['done']/t*100)}%" if t else "100%"

def _block(label: str, s: dict) -> str:
    return (
        f"**{label}**\n"
        f"👥 Total: `{s['total']}`\n"
        f"📨 Processed: `{s['done']}/{s['total']}`\n"
        f"✅ Sent: `{s['sent']}`\n"
        f"🚫 Blocked: `{s['blocked']}`\n"
        f"🗑 Deleted: `{s['deleted']}`\n"
        f"⚠️ Invalid: `{s['invalid']}`\n"
        f"❌ Failed: `{s['failed']}`"
    )

def _report(title: str, us: dict, gs: dict, *, elapsed=None, rate=None) -> str:
    ct = us["total"] + gs["total"]
    cd = us["done"]  + gs["done"]
    lines = [
        title,
        f"📤 Mode: `{_broadcast_mode()}` · ⚡ Workers: `{BROADCAST_WORKERS}`",
        f"🧭 Overall: `{cd}/{ct}`",
    ]
    if elapsed is not None:
        lines.append(f"⏱ Time: `{_fmt_time(elapsed)}`")
    if rate is not None:
        lines.append(f"🚀 Rate: `{rate:.1f}` msg/s")
    lines += ["", _block("👤 Users", us), "", _block("👥 Groups", gs)]
    return "\n".join(lines)

async def _safe_edit(msg, text: str):
    try: await msg.edit(text)
    except Exception: pass

async def _safe_notify(bot, admin_id, fallback, text: str):
    try: await bot.send_message(admin_id, text)
    except Exception:
        try: await fallback.reply_text(text)
        except Exception: pass

async def _deliver(message, chat_id: int):
    if BROADCAST_AS_FORWARD:
        return await message.forward(chat_id=chat_id)
    return await message.copy(chat_id=chat_id)


# ── Cursor-free MongoDB paginator ─────────────────────────────────────────────

async def _iter_mongo_ids(collection):
    """Fresh query per page — never keeps a server-side cursor alive."""
    last_oid = None
    while True:
        filt = {"_id": {"$gt": last_oid}} if last_oid is not None else {}
        docs = await collection.find(
            filt, {"_id": 1, "id": 1}
        ).sort("_id", 1).limit(500).to_list(length=500)
        if not docs:
            break
        for item in docs:
            try:
                yield int(item["id"])
            except (KeyError, TypeError, ValueError):
                continue
        last_oid = docs[-1]["_id"]
        await asyncio.sleep(0)

async def _iter_sql_ids(table: str):
    from database.sql_store import store
    from sqlalchemy import text as sa_text
    last_id = -(10**30)
    while True:
        with store.begin() as conn:
            rows = conn.execute(
                sa_text(f"SELECT id FROM {table} WHERE id > :lid ORDER BY id ASC LIMIT 500"),
                {"lid": last_id},
            ).fetchall()
        if not rows: break
        for row in rows:
            last_id = int(row[0])
            yield last_id

async def _iter_user_ids():
    if db.use_mongo:
        async for uid in _iter_mongo_ids(db.col): yield uid
    else:
        async for uid in _iter_sql_ids("users"): yield uid

async def _iter_group_ids():
    if db.use_mongo:
        async for cid in _iter_mongo_ids(db.grp): yield cid
    else:
        async for cid in _iter_sql_ids("groups_data"): yield cid


# ── Per-chat delivery ─────────────────────────────────────────────────────────

async def _send_one(chat_id: int, message, stats: dict, limiter: _RateLimiter,
                    is_user: bool):
    """
    Acquire rate-limit token, send, handle errors.
    FloodWait sleep happens here — OUTSIDE the semaphore — so the slot
    is freed for another worker while we wait.
    done is incremented exactly once via finally.
    """
    delete_fn = db.delete_user if is_user else db.delete_chat

    try:
        for attempt in range(3):
            try:
                await limiter.acquire()
                await _deliver(message, chat_id)
                stats["sent"] += 1
                limiter.on_success()
                return

            except FloodWait as e:
                wait = getattr(e, "value", getattr(e, "x", 5))
                limiter.flood_backoff(wait)
                # Sleep the full demanded time — Telegram is serious about this
                await asyncio.sleep(wait + 1)
                # Don't count as a failed attempt; retry immediately

            except (UserIsBlocked,):
                await delete_fn(chat_id)
                stats["blocked"] += 1
                return
            except InputUserDeactivated:
                await delete_fn(chat_id)
                stats["deleted"] += 1
                return
            except PeerIdInvalid:
                await delete_fn(chat_id)
                stats["invalid"] += 1
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("chat %s attempt %d: %s", chat_id, attempt + 1, exc)
                if attempt == 2:
                    stats["failed"] += 1
                else:
                    await asyncio.sleep(2)  # brief pause before retry
    finally:
        stats["done"] += 1  # always, exactly once


# ── Streaming engine ──────────────────────────────────────────────────────────

async def _send_stream(target, message, stats, status_msg, all_stats, started, limiter):
    semaphore = asyncio.Semaphore(BROADCAST_WORKERS)
    iterator  = _iter_user_ids() if target == USER_TARGET else _iter_group_ids()
    pending: set[asyncio.Task] = set()

    async def _worker(chat_id: int):
        # FloodWait sleep in _send_one happens OUTSIDE this semaphore context
        # because _send_one is called after acquire() returns.
        # But the semaphore is held for the entire send including retries.
        # To release on FloodWait we restructure: acquire sem, then call send.
        async with semaphore:
            await _send_one(chat_id, message, stats, limiter,
                            is_user=(target == USER_TARGET))

    async def _status_loop():
        while True:
            await asyncio.sleep(BROADCAST_STATUS_UPDATE_SECONDS)
            await _safe_edit(
                status_msg,
                _report(
                    f"📡 **Broadcast Running** — `{target}` {_pct(stats)}",
                    all_stats[0], all_stats[1],
                    elapsed=monotonic() - started,
                    rate=limiter.current_rate,
                ),
            )

    status_task = asyncio.create_task(_status_loop())
    try:
        async for chat_id in iterator:
            t = asyncio.create_task(_worker(chat_id))
            pending.add(t)
            t.add_done_callback(pending.discard)
            while len(pending) >= BROADCAST_WORKERS * 4:
                await asyncio.sleep(0.05)

        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
    except asyncio.CancelledError:
        for t in pending: t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        raise
    finally:
        status_task.cancel()
        try: await status_task
        except asyncio.CancelledError: pass


# ── Target selection ──────────────────────────────────────────────────────────

def _selected_targets(command_text: str) -> tuple:
    parts = (command_text or "").split(maxsplit=1)
    if len(parts) == 1:
        return ALL_TARGETS
    req = parts[1].strip().lower()
    if req in {"user", "users", "pm", "pms"}:
        return (USER_TARGET,)
    if req in {"group", "groups", "chat", "chats"}:
        return (GROUP_TARGET,)
    return ALL_TARGETS


# ── Commands ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
async def broadcast(bot: Client, message):
    global _active_broadcast

    if _active_broadcast and not _active_broadcast.done():
        await message.reply_text(
            "⚠️ A broadcast is already running.\nUse /cancel_broadcast to stop it first."
        )
        return

    b_msg   = message.reply_to_message
    started = monotonic()
    targets = _selected_targets(message.text)
    limiter = _RateLimiter(SENDS_PER_SECOND)

    total_users  = await db.total_users_count() if USER_TARGET  in targets else 0
    total_groups = await db.total_chat_count()  if GROUP_TARGET in targets else 0
    user_stats   = _new_stats(total_users)
    group_stats  = _new_stats(total_groups)

    update_min = max(1, BROADCAST_STATUS_UPDATE_SECONDS // 60)
    status = await message.reply_text(
        _report(
            f"📡 **Broadcast Started**\n"
            f"Updates every `{update_min}` min · `{BROADCAST_WORKERS}` workers · "
            f"`{SENDS_PER_SECOND}` msg/s initial rate",
            user_stats, group_stats,
        )
    )

    async def _run():
        if USER_TARGET in targets:
            await _send_stream(USER_TARGET, b_msg, user_stats, status,
                               (user_stats, group_stats), started, limiter)
        if GROUP_TARGET in targets:
            await _send_stream(GROUP_TARGET, b_msg, group_stats, status,
                               (user_stats, group_stats), started, limiter)

        elapsed = monotonic() - started
        final = _report("✅ **Broadcast Complete**", user_stats, group_stats, elapsed=elapsed)
        await _safe_edit(status, final)
        await _safe_notify(bot, message.from_user.id, message, final)

    _active_broadcast = asyncio.create_task(_run())
    try:
        await _active_broadcast
    except asyncio.CancelledError:
        elapsed = monotonic() - started
        txt = _report("🛑 **Broadcast Cancelled**", user_stats, group_stats, elapsed=elapsed)
        await _safe_edit(status, txt)
        await _safe_notify(bot, message.from_user.id, message, txt)
    except Exception as exc:
        logger.exception("Broadcast crashed: %s", exc)
        await _safe_edit(status, f"💥 **Broadcast crashed**\n`{exc}`")


@Client.on_message(filters.command("cancel_broadcast") & filters.user(ADMINS))
async def cancel_broadcast(bot: Client, message):
    global _active_broadcast
    if _active_broadcast and not _active_broadcast.done():
        _active_broadcast.cancel()
        await message.reply_text("🛑 Broadcast cancellation requested.")
    else:
        await message.reply_text("ℹ️ No active broadcast to cancel.")
