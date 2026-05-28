#  @MrMNTG @MusammilN 
from pyrogram import filters, Client
from pyrogram.types import Message
from utils import JOIN_REQUEST_USERS
from info import ADMINS
from database.users_chats_db import db

@Client.on_message(filters.command("clear_join_users") & filters.user(ADMINS))
async def clear_join_users(_, message: Message):
    JOIN_REQUEST_USERS.clear()
    await db.clear_join_users()
    await message.reply_text("✅ Cleared all join request users from MongoDB.")
