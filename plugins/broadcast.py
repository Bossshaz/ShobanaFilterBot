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

USER_TARGET = "users"
GROUP_TARGET = "groups"
ALL_TARGETS = (USER_TARGET, GROUP_TARGET)

# ── Tuning ────────────────────────────────────────────────────────────────────
# Parallel workers — each holds one in-flight Telegram send at a time.
BROADCAST_WORKERS = 25

# Global send rate (messages/second across ALL workers combined).
# Telegram's documented bot limit is ~30 msg/s. Stay safely under it.
# Lower to 20 if you still see frequent FloodWaits.
SENDS_PER_SECOND: float = 25.0

# On FloodWait: multiply current rate by this factor (halves it).
FLOOD_BACKOFF_FACTOR = 0.5
# After each successful send: multiply current rate by this (slow recovery).
FLOOD_RECOVER_FACTOR = 1.02
# ─────────────────────────────────────────────────────────────────────────────

# Tracks the running broadcast task so /cancel_broadcast works
_active_broadcast: asyncio.Task | None = None


# ── Token-bucket rate limiter ─────────────────────────────────────────────────

class _RateLimiter:
    """
    Adaptive token-bucket limiter shared by all workers.

    - Allows up to `rate` sends/second on average.
    - On FloodWait: backs off immediately (halves the rate).
    - After each success: gently recovers toward the configured ceiling.
    - All workers call `acquire()` before each send, so bursts are impossible.
    """

    def __init__(self, rate: float):
        self._rate = rate          # current effective rate (tokens/sec)
        self._max_rate = rate      # ceiling — never exceed this
        self._tokens = rate        # start full
        self._last_refill = monotonic()
        self._lock = asyncio.Lock()

    def _refill(self):
        now = monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
        self._last_refill = now

    async def acquire(self):
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate
            await asyncio.sleep(wait)

    def flood_backoff(self, flood_seconds: int):
        """Call when Telegram returns FloodWait."""
        self._rate = max(1.0, self._rate * FLOOD_BACKOFF_FACTOR)
        logger.warning(
            "FloodWait %ss — rate backed off to %.1f msg/s", flood_seconds, self._rate
        )

    def success(self):
        """Call after each successful send."""
        self._rate = min(self._max_rate, self._rate * FLOOD_RECOVER_FACTOR)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _broadcast_mode() -> str:
    return "forward" if BROADCAST_AS_FORWARD else "copy"


def _new_stats(total: int) -> dict:
    return {
        "total": total,
        "done": 0,
        "sent": 0,
        "blocked": 0,
        "deleted": 0,
        "invalid": 0,
        "failed": 0,
    }


def _format_duration(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def _pct(stats: dict) -> str:
    t = stats["total"]
    return f"{int(stats['done'] / t * 100)}%" if t else "100%"


def _stats_block(label: str, stats: dict) -> list[str]:
    return [
        f"**{label}**",
        f"👥 Total: `{stats['total']}`",
        f"📨 Processed: `{stats['done']}/{stats['total']}`",
        f"✅ Sent: `{stats['sent']}`",
        f"🚫 Blocked removed: `{stats['blocked']}`",
        f"🗑 Deleted removed: `{stats['deleted']}`",
        f"⚠️ Invalid removed: `{stats['invalid']}`",
        f"❌ Failed: `{stats['failed']}`",
    ]


def _report(
    title: str,
    user_stats: dict,
    group_stats: dict,
    *,
    elapsed: float | None = None,
    rate: float | None = None,
) -> str:
    ct = user_stats["total"] + group_stats["total"]
    cd = user_stats["done"] + group_stats["done"]
    lines = [
        title,
        f"📤 Mode: `{_broadcast_mode()}`  ⚡ Workers: `{BROADCAST_WORKERS}`",
        f"🧭 Overall: `{cd}/{ct}`",
    ]
    if elapsed is not None:
        lines.append(f"⏱ Time: `{_format_duration(elapsed)}`")
    if rate is not None:
        lines.append(f"🚀 Current rate: `{rate:.1f}` msg/s")
    lines += ["", *_stats_block("👤 Users", user_stats)]
    lines += ["", *_stats_block("👥 Groups", group_stats)]
    return "\n".join(lines)


async def _safe_edit(msg, text: str):
    try:
        await msg.edit(text)
    except Exception:
        pass


async def _safe_notify(bot: Client, admin_id: int, fallback, text: str):
    try:
        await bot.send_message(admin_id, text)
    except Exception:
        try:
            await fallback.reply_text(text)
        except Exception:
            pass


async def _deliver(message, chat_id: int):
    if BROADCAST_AS_FORWARD:
        return await message.forward(chat_id=chat_id)
    return await message.copy(chat_id=chat_id)


# ── Cursor-free MongoDB paginator ─────────────────────────────────────────────

async def _iter_mongo_ids(collection):
    """
    Paginates with fresh queries instead of a long-lived cursor.
    Atlas kills cursors after 10 min; this never keeps one open between pages.
    """
    BATCH = 500
    last_oid = None

    while True:
        filt = {"_id": {"$gt": last_oid}} if last_oid is not None else {}
        docs = await collection.find(
            filt, {"_id": 1, "id": 1}
        ).sort("_id", 1).limit(BATCH).to_list(length=BATCH)

        if not docs:
            break

        for item in docs:
            try:
                yield int(item["id"])
            except (KeyError, TypeError, ValueError):
                continue

        last_oid = docs[-1]["_id"]
        await asyncio.sleep(0)  # yield to event loop between pages


async def _iter_sql_ids(table: str):
    from database.sql_store import store
    from sqlalchemy import text as sa_text

    last_id = -(10 ** 30)
    while True:
        with store.begin() as conn:
            rows = conn.execute(
                sa_text(f"SELECT id FROM {table} WHERE id > :lid ORDER BY id ASC LIMIT 500"),
                {"lid": last_id},
            ).fetchall()
        if not rows:
            break
        for row in rows:
            last_id = int(row[0])
            yield last_id


async def _iter_user_ids():
    if db.use_mongo:
        async for uid in _iter_mongo_ids(db.col):
            yield uid
    else:
        async for uid in _iter_sql_ids("users"):
            yield uid


async def _iter_group_ids():
    if db.use_mongo:
        async for cid in _iter_mongo_ids(db.grp):
            yield cid
    else:
        async for cid in _iter_sql_ids("groups_data"):
            yield cid


# ── Per-chat delivery ─────────────────────────────────────────────────────────

async def _send_to_user(user_id: int, message, stats: dict, limiter: _RateLimiter):
    """
    FIX: `done` incremented exactly once — at the very end, regardless of outcome.
    FIX: FloodWait notifies the shared limiter (backs off all workers), then retries.
    """
    try:
        for attempt in range(3):
            try:
                await limiter.acquire()
                await _deliver(message, user_id)
                stats["sent"] += 1
                limiter.success()
                return
            except FloodWait as e:
                wait = getattr(e, "value", getattr(e, "x", 5))
                limiter.flood_backoff(wait)
                await asyncio.sleep(wait)
            except UserIsBlocked:
                await db.delete_user(user_id)
                stats["blocked"] += 1
                return
            except InputUserDeactivated:
                await db.delete_user(user_id)
                stats["deleted"] += 1
                return
            except PeerIdInvalid:
                await db.delete_user(user_id)
                stats["invalid"] += 1
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("User %s attempt %d failed: %s", user_id, attempt + 1, exc)
                if attempt == 2:
                    stats["failed"] += 1
    finally:
        stats["done"] += 1   # ← always runs, exactly once


async def _send_to_group(chat_id: int, message, stats: dict, limiter: _RateLimiter):
    try:
        for attempt in range(3):
            try:
                await limiter.acquire()
                await _deliver(message, chat_id)
                stats["sent"] += 1
                limiter.success()
                return
            except FloodWait as e:
                wait = getattr(e, "value", getattr(e, "x", 5))
                limiter.flood_backoff(wait)
                await asyncio.sleep(wait)
            except PeerIdInvalid:
                await db.delete_chat(chat_id)
                stats["invalid"] += 1
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Group %s attempt %d failed: %s", chat_id, attempt + 1, exc)
                if attempt == 2:
                    stats["failed"] += 1
    finally:
        stats["done"] += 1


# ── Concurrent streaming engine ───────────────────────────────────────────────

async def _send_stream(
    target: str,
    message,
    stats: dict,
    status_msg,
    all_stats: tuple[dict, dict],
    started: float,
    limiter: _RateLimiter,
):
    semaphore = asyncio.Semaphore(BROADCAST_WORKERS)
    iterator = _iter_user_ids() if target == USER_TARGET else _iter_group_ids()
    sender = _send_to_user if target == USER_TARGET else _send_to_group
    pending: set[asyncio.Task] = set()

    async def _worker(chat_id: int):
        async with semaphore:
            await sender(chat_id, message, stats, limiter)

    async def _status_loop():
        while True:
            await asyncio.sleep(BROADCAST_STATUS_UPDATE_SECONDS)
            await _safe_edit(
                status_msg,
                _report(
                    f"📡 **Broadcast Running**  —  target: `{target}`  {_pct(stats)}",
                    all_stats[0], all_stats[1],
                    elapsed=monotonic() - started,
                    rate=limiter._rate,
                ),
            )

    status_task = asyncio.create_task(_status_loop())

    try:
        async for chat_id in iterator:
            task = asyncio.create_task(_worker(chat_id))
            pending.add(task)
            task.add_done_callback(pending.discard)

            # Backpressure: don't enqueue more than workers*4 ahead
            while len(pending) >= BROADCAST_WORKERS * 4:
                await asyncio.sleep(0.05)

        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
    except asyncio.CancelledError:
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        raise
    finally:
        status_task.cancel()
        try:
            await status_task
        except asyncio.CancelledError:
            pass


# ── Target selection ──────────────────────────────────────────────────────────

async def _selected_targets(command_text: str) -> tuple[str, ...]:
    parts = (command_text or "").split(maxsplit=1)
    if len(parts) == 1:
        return ALL_TARGETS
    req = parts[1].strip().lower()
    if req in {"user", "users", "pm", "pms"}:
        return (USER_TARGET,)
    if req in {"group", "groups", "chat", "chats"}:
        return (GROUP_TARGET,)
    return ALL_TARGETS


# ── Command handlers ──────────────────────────────────────────────────────────

@Client.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
async def broadcast(bot: Client, message):
    global _active_broadcast

    if _active_broadcast and not _active_broadcast.done():
        await message.reply_text(
            "⚠️ A broadcast is already running.\nUse /cancel_broadcast to stop it first."
        )
        return

    b_msg = message.reply_to_message
    started = monotonic()
    targets = await _selected_targets(message.text)
    limiter = _RateLimiter(SENDS_PER_SECOND)

    total_users = await db.total_users_count() if USER_TARGET in targets else 0
    total_groups = await db.total_chat_count() if GROUP_TARGET in targets else 0
    user_stats = _new_stats(total_users)
    group_stats = _new_stats(total_groups)

    update_min = max(1, BROADCAST_STATUS_UPDATE_SECONDS // 60)
    status = await message.reply_text(
        _report(
            f"📡 **Broadcast Started**\n"
            f"Updates every `{update_min}` min · `{BROADCAST_WORKERS}` workers · `{SENDS_PER_SECOND}` msg/s",
            user_stats, group_stats,
        )
    )

    async def _run():
        if USER_TARGET in targets:
            await _send_stream(
                USER_TARGET, b_msg, user_stats, status,
                (user_stats, group_stats), started, limiter,
            )
        if GROUP_TARGET in targets:
            await _send_stream(
                GROUP_TARGET, b_msg, group_stats, status,
                (user_stats, group_stats), started, limiter,
            )

        elapsed = monotonic() - started
        final = _report("✅ **Broadcast Complete**", user_stats, group_stats, elapsed=elapsed)
        await _safe_edit(status, final)
        await _safe_notify(bot, message.from_user.id, message, final)

    _active_broadcast = asyncio.create_task(_run())

    try:
        await _active_broadcast
    except asyncio.CancelledError:
        elapsed = monotonic() - started
        cancelled = _report(
            "🛑 **Broadcast Cancelled**", user_stats, group_stats, elapsed=elapsed
        )
        await _safe_edit(status, cancelled)
        await _safe_notify(bot, message.from_user.id, message, cancelled)
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
