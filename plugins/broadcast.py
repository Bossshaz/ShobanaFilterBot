import asyncio
import logging
from time import monotonic

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, UserIsBlocked, InputUserDeactivated, PeerIdInvalid

from database.users_chats_db import db
from info import ADMINS, BROADCAST_AS_FORWARD

logger = logging.getLogger(__name__)

MAX_CONCURRENT = 60
CHUNK_SIZE = 100
PROGRESS_UPDATE_SECONDS = 5
TRANSIENT_RETRIES = 3


def _broadcast_mode() -> str:
    return "forward" if BROADCAST_AS_FORWARD else "copy"


def _empty_stats(total: int) -> dict:
    return {
        "total": total,
        "done": 0,
        "success": 0,
        "blocked": 0,
        "deleted": 0,
        "unavailable": 0,
        "failed": 0,
    }


def _progress_bar(done: int, total: int) -> str:
    pct = int(done / total * 100) if total else 100
    filled = min(10, pct // 10)
    return f"{pct}% `{'█' * filled}{'░' * (10 - filled)}`"


def _summary(stats: dict, *, title: str = "📊 Broadcast Report", elapsed: float | None = None) -> str:
    lines = [
        f"**{title}**",
        f"👥 Total: `{stats['total']}`",
        f"📨 Confirmed: `{stats['done']}/{stats['total']}`",
        f"✅ Sent: `{stats['success']}`",
        f"🚫 Blocked: `{stats['blocked']}`",
        f"🗑 Deleted: `{stats['deleted']}`",
        f"⚠️ Unavailable: `{stats['unavailable']}`",
    ]
    if stats["failed"]:
        lines.append(f"❌ Failed after retry: `{stats['failed']}`")
    if elapsed is not None:
        lines.append(f"⏱ Time: `{elapsed:.1f}s`")
    lines.append(f"📤 Mode: `{_broadcast_mode()}`")
    return "\n".join(lines)


async def _safe_edit(msg, text):
    try:
        await msg.edit(text)
    except Exception:
        pass


async def _deliver(message, chat_id: int):
    if BROADCAST_AS_FORWARD:
        return await message.forward(chat_id=chat_id)
    return await message.copy(chat_id=chat_id)


async def _send_one(sem, chat_id: int, message, stats: dict, *, is_user: bool):
    async with sem:
        attempts = 0
        while True:
            try:
                await _deliver(message, chat_id)
                stats["success"] += 1
                return
            except FloodWait as e:
                await asyncio.sleep(getattr(e, "value", getattr(e, "x", 0)) + 1)
            except UserIsBlocked:
                if is_user:
                    await db.delete_user(chat_id)
                stats["blocked"] += 1
                return
            except InputUserDeactivated:
                if is_user:
                    await db.delete_user(chat_id)
                stats["deleted"] += 1
                return
            except PeerIdInvalid:
                if is_user:
                    await db.delete_user(chat_id)
                else:
                    await db.delete_chat(chat_id)
                stats["unavailable"] += 1
                return
            except Exception as e:
                attempts += 1
                if attempts <= TRANSIENT_RETRIES:
                    await asyncio.sleep(min(2 * attempts, 10))
                    continue
                logger.exception("Broadcast failed for %s after retries: %s", chat_id, e)
                if not is_user:
                    await db.delete_chat(chat_id)
                stats["failed"] += 1
                return


async def _tracked_send(sem, chat_id: int, message, stats: dict, *, is_user: bool):
    try:
        await _send_one(sem, chat_id, message, stats, is_user=is_user)
    finally:
        stats["done"] += 1


async def _run_broadcast(chat_ids, b_msg, sts_msg, *, is_user: bool):
    total = len(chat_ids)
    stats = _empty_stats(total)
    started = monotonic()
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    async def _progress_updater():
        while stats["done"] < total:
            await asyncio.sleep(PROGRESS_UPDATE_SECONDS)
            await _safe_edit(
                sts_msg,
                f"📡 **Broadcasting...** {_progress_bar(stats['done'], total)}\n\n"
                + _summary(stats, title="Live Status"),
            )

    updater = asyncio.create_task(_progress_updater())
    try:
        tasks = [_tracked_send(sem, chat_id, b_msg, stats, is_user=is_user) for chat_id in chat_ids]
        for start in range(0, len(tasks), CHUNK_SIZE):
            await asyncio.gather(*tasks[start:start + CHUNK_SIZE], return_exceptions=True)
    finally:
        updater.cancel()

    elapsed = monotonic() - started
    return stats, elapsed


async def _load_ids(cursor, key: str) -> list[int]:
    rows = [item async for item in cursor] if hasattr(cursor, "__aiter__") else cursor
    ids = []
    for item in rows:
        try:
            ids.append(int(item[key]))
        except (KeyError, TypeError, ValueError):
            continue
    return ids


@Client.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
async def broadcast(bot, message):
    b_msg = message.reply_to_message
    sts = await message.reply_text(f"⏳ Loading users for `{_broadcast_mode()}` broadcast...")

    users = await _load_ids(await db.get_all_users(), "id")
    await _safe_edit(sts, f"📡 Starting broadcast to `{len(users)}` users...")

    stats, elapsed = await _run_broadcast(users, b_msg, sts, is_user=True)

    await _safe_edit(
        sts,
        "✅ **User Broadcast Complete**\n\n"
        + _summary(stats, title="Final Report", elapsed=elapsed),
    )


@Client.on_message(filters.command("grpbroadcast") & filters.user(ADMINS) & filters.reply)
async def grpbroadcast(bot, message):
    b_msg = message.reply_to_message
    sts = await message.reply_text(f"⏳ Loading groups for `{_broadcast_mode()}` broadcast...")

    chats = await _load_ids(await db.get_all_chats(), "id")
    await _safe_edit(sts, f"📡 Starting broadcast to `{len(chats)}` groups...")

    stats, elapsed = await _run_broadcast(chats, b_msg, sts, is_user=False)

    await _safe_edit(
        sts,
        "✅ **Group Broadcast Complete**\n\n"
        + _summary(stats, title="Final Report", elapsed=elapsed),
    )
