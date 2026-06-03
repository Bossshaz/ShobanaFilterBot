import asyncio
import logging
from time import monotonic

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, UserIsBlocked, InputUserDeactivated, PeerIdInvalid

from database.users_chats_db import db
from info import (
    ADMINS,
    BROADCAST_AS_FORWARD,
    BROADCAST_SLEEP_SECONDS,
    BROADCAST_STATUS_UPDATE_SECONDS,
)

logger = logging.getLogger(__name__)

USER_TARGET = "users"
GROUP_TARGET = "groups"
ALL_TARGETS = (USER_TARGET, GROUP_TARGET)


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
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {sec}s"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def _progress_line(stats: dict) -> str:
    total = stats["total"]
    pct = int(stats["done"] / total * 100) if total else 100
    return f"{pct}% complete · `{stats['done']}/{total}` confirmed"


def _stats_lines(label: str, stats: dict) -> list[str]:
    return [
        f"**{label}**",
        f"👥 Total: `{stats['total']}`",
        f"📨 Confirmed: `{stats['done']}/{stats['total']}`",
        f"✅ Sent: `{stats['sent']}`",
        f"🚫 Blocked removed: `{stats['blocked']}`",
        f"🗑 Deleted removed: `{stats['deleted']}`",
        f"⚠️ Invalid removed: `{stats['invalid']}`",
        f"❌ Failed: `{stats['failed']}`",
    ]


def _build_report(title: str, user_stats: dict, group_stats: dict, *, elapsed: float | None = None) -> str:
    combined_total = user_stats["total"] + group_stats["total"]
    combined_done = user_stats["done"] + group_stats["done"]
    lines = [
        title,
        f"📤 Mode: `{_broadcast_mode()}`",
        f"🧭 Overall: `{combined_done}/{combined_total}`",
    ]
    if elapsed is not None:
        lines.append(f"⏱ Time: `{_format_duration(elapsed)}`")
    lines.extend(["", *_stats_lines("👤 Users", user_stats), "", *_stats_lines("👥 Groups", group_stats)])
    return "\n".join(lines)


async def _safe_edit(message, text: str):
    try:
        await message.edit(text)
    except Exception:
        pass


async def _safe_admin_message(bot: Client, admin_id: int, fallback_message, text: str):
    try:
        await bot.send_message(admin_id, text)
    except Exception:
        await fallback_message.reply_text(text)


async def _deliver(message, chat_id: int):
    if BROADCAST_AS_FORWARD:
        return await message.forward(chat_id=chat_id)
    return await message.copy(chat_id=chat_id)


async def _iter_mongo_ids(collection):
    cursor = collection.find({}, {"_id": 0, "id": 1}).batch_size(1000)
    async for item in cursor:
        try:
            yield int(item["id"])
        except (KeyError, TypeError, ValueError):
            continue


async def _iter_sql_ids(table: str):
    from database.sql_store import store
    from sqlalchemy import text

    last_id = -10**30
    while True:
        with store.begin() as conn:
            rows = conn.execute(
                text(f"SELECT id FROM {table} WHERE id > :last_id ORDER BY id ASC LIMIT 1000"),
                {"last_id": last_id},
            ).fetchall()
        if not rows:
            break
        for row in rows:
            last_id = int(row[0])
            yield last_id


async def _iter_user_ids():
    if db.use_mongo:
        async for user_id in _iter_mongo_ids(db.col):
            yield user_id
        return
    async for user_id in _iter_sql_ids("users"):
        yield user_id


async def _iter_group_ids():
    if db.use_mongo:
        async for chat_id in _iter_mongo_ids(db.grp):
            yield chat_id
        return
    async for chat_id in _iter_sql_ids("groups_data"):
        yield chat_id


async def _send_to_user(user_id: int, message, stats: dict):
    while True:
        try:
            await _deliver(message, user_id)
            stats["sent"] += 1
            break
        except FloodWait as e:
            await asyncio.sleep(getattr(e, "value", getattr(e, "x", 0)) + 1)
        except UserIsBlocked:
            await db.delete_user(user_id)
            stats["blocked"] += 1
            break
        except InputUserDeactivated:
            await db.delete_user(user_id)
            stats["deleted"] += 1
            break
        except PeerIdInvalid:
            await db.delete_user(user_id)
            stats["invalid"] += 1
            break
        except Exception as e:
            logger.warning("Broadcast failed for user %s: %s", user_id, e)
            stats["failed"] += 1
            break
    stats["done"] += 1


async def _send_to_group(chat_id: int, message, stats: dict):
    while True:
        try:
            await _deliver(message, chat_id)
            stats["sent"] += 1
            break
        except FloodWait as e:
            await asyncio.sleep(getattr(e, "value", getattr(e, "x", 0)) + 1)
        except PeerIdInvalid:
            await db.delete_chat(chat_id)
            stats["invalid"] += 1
            break
        except Exception as e:
            logger.warning("Broadcast failed for group %s: %s", chat_id, e)
            stats["failed"] += 1
            break
    stats["done"] += 1


async def _send_stream(target: str, message, stats: dict, status_message, all_stats: tuple[dict, dict], started: float):
    last_update = monotonic()
    iterator = _iter_user_ids() if target == USER_TARGET else _iter_group_ids()
    sender = _send_to_user if target == USER_TARGET else _send_to_group
    async for chat_id in iterator:
        await sender(chat_id, message, stats)
        if BROADCAST_SLEEP_SECONDS > 0:
            await asyncio.sleep(BROADCAST_SLEEP_SECONDS)
        now = monotonic()
        if now - last_update >= BROADCAST_STATUS_UPDATE_SECONDS:
            await _safe_edit(
                status_message,
                _build_report(
                    f"📡 **Broadcast Running**\nCurrent target: `{target}` · {_progress_line(stats)}",
                    all_stats[0],
                    all_stats[1],
                    elapsed=now - started,
                ),
            )
            last_update = now


async def _selected_targets(command_text: str) -> tuple[str, ...]:
    parts = (command_text or "").split(maxsplit=1)
    if len(parts) == 1:
        return ALL_TARGETS
    requested = parts[1].strip().lower()
    if requested in {"user", "users", "pm", "pms"}:
        return (USER_TARGET,)
    if requested in {"group", "groups", "chat", "chats"}:
        return (GROUP_TARGET,)
    return ALL_TARGETS


@Client.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
async def broadcast(bot: Client, message):
    b_msg = message.reply_to_message
    started = monotonic()
    targets = await _selected_targets(message.text)

    total_users = await db.total_users_count() if USER_TARGET in targets else 0
    total_groups = await db.total_chat_count() if GROUP_TARGET in targets else 0
    user_stats = _new_stats(total_users)
    group_stats = _new_stats(total_groups)

    status = await message.reply_text(
        _build_report(
            "📡 **Broadcast Started**\n"
            f"Status updates every `{BROADCAST_STATUS_UPDATE_SECONDS // 60}` minutes.\n"
            "Delivery is sequential to avoid Telegram limits.",
            user_stats,
            group_stats,
        )
    )

    if USER_TARGET in targets:
        await _send_stream(USER_TARGET, b_msg, user_stats, status, (user_stats, group_stats), started)
    if GROUP_TARGET in targets:
        await _send_stream(GROUP_TARGET, b_msg, group_stats, status, (user_stats, group_stats), started)

    elapsed = monotonic() - started
    final_report = _build_report("✅ **Broadcast Complete**", user_stats, group_stats, elapsed=elapsed)
    await _safe_edit(status, final_report)
    await _safe_admin_message(bot, message.from_user.id, message, final_report)
