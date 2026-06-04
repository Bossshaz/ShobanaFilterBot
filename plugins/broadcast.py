from pyrogram import Client, filters
from pyrogram.errors import FloodWait, UserIsBlocked, InputUserDeactivated, PeerIdInvalid
import asyncio
from database.users_chats_db import db
from info import ADMINS

MAX_CONCURRENT = 20
CHUNK_SIZE = 100
SEND_ATTEMPTS = 3
AUTO_RETRY_ROUNDS = 2
RETRY_DELAY = 5

# Central store — one entry per active broadcast
# Keys: "msg", "users" (set of failed ids), "stats" (latest report counters)
BC = {}


# ─────────────────────────── helpers ────────────────────────────

def _blank_stats():
    return {"done": 0, "success": 0, "blocked": 0, "deleted": 0, "failed": 0}


def _summary(stats: dict, extra: str = "") -> str:
    lines = [
        "📊 **Broadcast Report**",
        f"✅ Success : `{stats['success']}`",
        f"❌ Failed  : `{stats['failed']}`",
        f"🚫 Blocked : `{stats['blocked']}`",
        f"🗑 Deleted : `{stats['deleted']}`",
    ]
    if extra:
        lines.append(extra)
    return "\n".join(lines)


async def _safe_edit(msg, text, reply_markup=None):
    """Edit a message, silently ignoring 'message not modified' errors."""
    try:
        await msg.edit(text, reply_markup=reply_markup)
    except Exception:
        pass


async def _sleep_flood(e: FloodWait):
    """Sleep for the FloodWait value exposed by the installed Pyrogram fork."""
    wait_for = getattr(e, "value", None) or getattr(e, "x", 0)
    await asyncio.sleep(int(wait_for) + 1)


async def _copy_to_user(user_id, b_msg):
    """
    Copy a message to one user and return a status string.
    FloodWait is not a failure: the bot waits and automatically resumes.
    """
    while True:
        try:
            await b_msg.copy(chat_id=user_id)
            return "Success"
        except FloodWait as e:
            await _sleep_flood(e)
        except UserIsBlocked:
            await db.delete_user(user_id)
            return "Blocked"
        except (InputUserDeactivated, PeerIdInvalid):
            await db.delete_user(user_id)
            return "Deleted"
        except Exception:
            return "Error"


async def _copy_to_user_with_retries(user_id, b_msg, attempts=SEND_ATTEMPTS):
    """Retry temporary copy failures before marking a user as failed."""
    for attempt in range(1, attempts + 1):
        status = await _copy_to_user(user_id, b_msg)
        if status != "Error" or attempt == attempts:
            return status
        await asyncio.sleep(RETRY_DELAY * attempt)
    return "Error"


def _record_status(stats: dict, user_id, status: str, failed_ids: set):
    stats["done"] += 1
    if status == "Success":
        stats["success"] += 1
    elif status == "Blocked":
        stats["blocked"] += 1
    elif status == "Deleted":
        stats["deleted"] += 1
    else:
        stats["failed"] += 1
        failed_ids.add(user_id)


# ─────────────── core sender (never-fail on FloodWait) ──────────

async def _send_one(sem, user_id, b_msg, stats: dict, failed_ids: set):
    async with sem:
        status = await _copy_to_user_with_retries(user_id, b_msg)
        _record_status(stats, user_id, status, failed_ids)


async def _send_group_one(sem, chat_id, b_msg, stats: dict):
    async with sem:
        while True:
            try:
                await b_msg.copy(chat_id=chat_id)
                stats["done"] += 1
                stats["success"] += 1
                return
            except FloodWait as e:
                await _sleep_flood(e)
            except Exception:
                await db.delete_chat(chat_id)
                stats["done"] += 1
                stats["failed"] += 1
                return


# ─────────────── live-progress runner ───────────────────────────

async def _run_broadcast(tasks, stats, sts_msg, total, update_interval=5):
    """
    Run tasks in chunks and edit the status message every `update_interval` seconds.
    """
    async def _progress_updater():
        while True:
            await asyncio.sleep(update_interval)
            pct = int(stats["done"] / total * 100) if total else 100
            bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
            await _safe_edit(
                sts_msg,
                f"📡 **Broadcasting…** {pct}%\n`{bar}`\n\n"
                + _summary(stats)
            )

    updater = asyncio.create_task(_progress_updater())
    try:
        for i in range(0, len(tasks), CHUNK_SIZE):
            await asyncio.gather(*tasks[i:i + CHUNK_SIZE], return_exceptions=True)
    finally:
        updater.cancel()


async def _auto_retry_failed(sts_msg, b_msg, stats, failed_ids: set, total):
    """Automatically retry failed users and keep the status message updated."""
    retry_notes = []
    for round_no in range(1, AUTO_RETRY_ROUNDS + 1):
        if not failed_ids:
            break

        failed_snapshot = list(failed_ids)
        retry_stats = _blank_stats()
        sem = asyncio.Semaphore(MAX_CONCURRENT)

        await _safe_edit(
            sts_msg,
            f"🔁 **Auto retry {round_no}/{AUTO_RETRY_ROUNDS}** for "
            f"`{len(failed_snapshot)}` failed users…\n\n" + _summary(stats)
        )

        async def _retry_one(uid):
            async with sem:
                status = await _copy_to_user_with_retries(uid, b_msg)
                retry_stats["done"] += 1

                if status == "Error":
                    retry_stats["failed"] += 1
                    return

                failed_ids.discard(uid)
                if stats["failed"] > 0:
                    stats["failed"] -= 1

                if status == "Success":
                    stats["success"] += 1
                    retry_stats["success"] += 1
                elif status == "Blocked":
                    stats["blocked"] += 1
                    retry_stats["blocked"] += 1
                elif status == "Deleted":
                    stats["deleted"] += 1
                    retry_stats["deleted"] += 1

        await asyncio.gather(*[_retry_one(uid) for uid in failed_snapshot], return_exceptions=True)

        retry_notes.append(
            f"🔁 Auto retry {round_no}: recovered `{retry_stats['success']}`, "
            f"cleaned `{retry_stats['blocked'] + retry_stats['deleted']}`, "
            f"still failed `{len(failed_ids)}`"
        )

        if failed_ids and round_no < AUTO_RETRY_ROUNDS:
            await asyncio.sleep(RETRY_DELAY)

    status = "✅ **Broadcast Complete**" if not failed_ids else "⚠️ **Broadcast Complete**"
    extra = "\n\n" + "\n".join(retry_notes) if retry_notes else ""
    await _safe_edit(
        sts_msg,
        f"{status} — {total} users\n\n" + _summary(stats, extra=extra)
    )


# ─────────────────────── /broadcast ─────────────────────────────

@Client.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
async def broadcast(bot, message):
    b_msg = message.reply_to_message
    sts = await message.reply_text("⏳ Loading users…")

    users = await db.get_all_users()
    users = [u async for u in users] if hasattr(users, "__aiter__") else users
    total = len(users)

    # Reset global state for this broadcast
    BC.clear()
    BC["msg"] = b_msg
    failed_ids = set()
    BC["users"] = failed_ids

    stats = _blank_stats()
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = [_send_one(sem, int(u["id"]), b_msg, stats, failed_ids) for u in users]

    await _run_broadcast(tasks, stats, sts, total)

    BC["stats"] = stats
    await _auto_retry_failed(sts, b_msg, stats, failed_ids, total)


# ─────────────────── /grpbroadcast ──────────────────────────────

@Client.on_message(filters.command("grpbroadcast") & filters.user(ADMINS) & filters.reply)
async def grpbroadcast(bot, message):
    b_msg = message.reply_to_message
    sts = await message.reply_text("⏳ Loading groups…")

    chats = await db.get_all_chats()
    chats = [c async for c in chats] if hasattr(chats, "__aiter__") else chats
    total = len(chats)

    stats = _blank_stats()
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = [_send_group_one(sem, int(c["id"]), b_msg, stats) for c in chats]

    await _run_broadcast(tasks, stats, sts, total)

    await _safe_edit(
        sts,
        f"✅ **Group Broadcast Complete** — {total} chats\n\n" + _summary(stats)
    )
