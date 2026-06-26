import time
import logging
from collections import defaultdict
from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from database.ia_filterdb import Media
from utils import get_size

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Trending state — resets every 24 hours automatically
# ---------------------------------------------------------------------------
_TREND_HITS  = defaultdict(int)   # file_id -> download count
_TREND_META  = {}                  # file_id -> (file_name, file_size)
_TREND_RESET = time.time() + 86400


def _maybe_reset():
    global _TREND_RESET
    if time.time() > _TREND_RESET:
        _TREND_HITS.clear()
        _TREND_META.clear()
        _TREND_RESET = time.time() + 86400


def track_file_hit(file_id: str, file_name: str, file_size):
    """Call this every time a file is sent to a user."""
    _maybe_reset()
    _TREND_HITS[file_id] += 1
    _TREND_META[file_id] = (file_name or '', file_size or 0)


# ---------------------------------------------------------------------------
# /new — last 10 indexed files
# ---------------------------------------------------------------------------
@Client.on_message(filters.command("new") & (filters.group | filters.private))
async def new_files(client, message):
    try:
        cursor = await Media.collection.find()
        cursor = cursor.sort('$natural', -1).limit(10)
        files = await cursor.to_list(length=10)
    except Exception as e:
        logger.exception(e)
        return await message.reply("❌ Failed to fetch files.")

    if not files:
        return await message.reply("No files in database yet.")

    btn = [
        [InlineKeyboardButton(
            text=f"📂 {get_size(f.file_size)} — {(f.file_name or 'Unknown')[:40]}",
            callback_data=f"file#{f.file_id}"
        )]
        for f in files
    ]
    await message.reply(
        "🆕 <b>Recently Added Files</b>",
        reply_markup=InlineKeyboardMarkup(btn),
        parse_mode=enums.ParseMode.HTML
    )


# ---------------------------------------------------------------------------
# /trending — top 5 most downloaded files in last 24 hours
# ---------------------------------------------------------------------------
@Client.on_message(filters.command("trending") & (filters.group | filters.private))
async def trending_files(client, message):
    _maybe_reset()
    if not _TREND_HITS:
        return await message.reply(
            "📊 No trending data yet. File downloads will appear here within 24h."
        )

    top5 = sorted(_TREND_HITS.items(), key=lambda x: x[1], reverse=True)[:5]
    btn = []
    for file_id, count in top5:
        fname, fsize = _TREND_META.get(file_id, ('Unknown', 0))
        btn.append([InlineKeyboardButton(
            text=f"🔥 {count}×  {fname[:38]}",
            callback_data=f"file#{file_id}"
        )])

    await message.reply(
        "🔥 <b>Trending — Last 24 Hours</b>",
        reply_markup=InlineKeyboardMarkup(btn),
        parse_mode=enums.ParseMode.HTML
    )
