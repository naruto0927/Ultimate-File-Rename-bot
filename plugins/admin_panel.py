"""
plugins/admin_panel.py
───────────────────────
Admin-only commands: status, restart, ping, broadcast, ban, unban.
Rimuru Tempest themed UI.
"""

import os
import sys
import time
import asyncio
import datetime
import logging

from config import Config
from pyrogram import Client, filters
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from pyrogram.errors import FloodWait, InputUserDeactivated, UserIsBlocked, PeerIdInvalid

from helper.database import jishubotz
from helper.ui import RUI

logger = logging.getLogger(__name__)


# ── /status ───────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("status") & filters.user(Config.ADMIN))
async def get_stats(bot: Client, message: Message):
    total_users = await jishubotz.total_users_count()
    uptime      = time.strftime("%Hh %Mm %Ss", time.gmtime(time.time() - bot.uptime))
    start_t     = time.time()
    st          = await message.reply_text("💠 <i>Great Sage analyzing…</i>")
    ping_ms     = (time.time() - start_t) * 1000
    await st.edit_text(
        "╭━━━〔 🌌 TEMPEST STATUS 〕━━━╮\n"
        f"┃  ⏱   Uptime  ·  {uptime}\n"
        f"┃  🏓  Ping    ·  {ping_ms:.1f} ms\n"
        f"┃  👥  Users   ·  {total_users:,}\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
    )


# ── /restart ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("restart") & filters.user(Config.ADMIN))
async def restart_bot(bot: Client, message: Message):
    msg = await message.reply_text(
        "╭━━━〔 🔄 SYSTEM RESTART 〕━━━╮\n"
        "┃  🌀  Magicules resetting…\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
    )
    await asyncio.sleep(2)
    await msg.edit_text(
        "╭━━━〔 ✨ TEMPEST ONLINE 〕━━━╮\n"
        "┃  ⚡  System back online.\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
    )
    os.execl(sys.executable, sys.executable, *sys.argv)


# ── /ping ─────────────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("ping"))
async def ping(_, message: Message):
    start_t = time.time()
    rm      = await message.reply_text("🏓 <i>Pinging Great Sage…</i>")
    ms_took = (time.time() - start_t) * 1000
    await rm.edit_text(
        "╭━━━〔 🏓 PONG 〕━━━╮\n"
        f"┃  ⚡  Response  ·  {ms_took:.1f} ms\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
    )


# ── /broadcast ────────────────────────────────────────────────────────────────

@Client.on_message(
    filters.command("broadcast") & filters.user(Config.ADMIN) & filters.reply
)
async def broadcast_handler(bot: Client, m: Message):
    broadcast_msg = m.reply_to_message
    sts_msg       = await m.reply_text(
        "╭━━━〔 📢 TEMPEST BROADCAST 〕━━━╮\n"
        "┃  🌌  Initiating transmission…\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
    )
    start_time  = time.time()
    total_users = await jishubotz.total_users_count()

    done = success = failed = 0
    sem  = asyncio.Semaphore(25)

    async def _send_one(user_id):
        nonlocal done, success, failed
        async with sem:
            sts = await _send_msg(user_id, broadcast_msg)
        if sts == 200:
            success += 1
        else:
            failed += 1
        if sts == 400:
            await jishubotz.delete_user(user_id)
        done += 1
        if done % 20 == 0:
            pct = int(done / total_users * 10) if total_users else 0
            bar = "█" * pct + "░" * (10 - pct)
            try:
                await sts_msg.edit_text(
                    "╭━━━〔 📢 BROADCAST ACTIVE 〕━━━╮\n"
                    f"┃  <code>[{bar}]</code>  {done}/{total_users}\n"
                    f"┃  ✅  Sent    ·  {success:,}\n"
                    f"┃  ❌  Failed  ·  {failed:,}\n"
                    "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
                )
            except Exception:
                pass

    tasks = []
    async for user in await jishubotz.get_all_users():
        tasks.append(asyncio.create_task(_send_one(user["_id"])))

    await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = datetime.timedelta(seconds=int(time.time() - start_time))
    await sts_msg.edit_text(
        "╭━━━〔 📢 BROADCAST COMPLETE 〕━━━╮\n"
        f"┃  ⏱   Duration  ·  {elapsed}\n"
        f"┃  👥  Total     ·  {total_users:,}\n"
        f"┃  ✅  Success   ·  {success:,}\n"
        f"┃  ❌  Failed    ·  {failed:,}\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
    )


async def _send_msg(user_id: int, message: Message) -> int:
    try:
        await message.copy(chat_id=int(user_id))
        return 200
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await _send_msg(user_id, message)
    except (InputUserDeactivated, UserIsBlocked, PeerIdInvalid):
        return 400
    except Exception as e:
        logger.error(f"[broadcast] {user_id}: {e}")
        return 500


# ── /ban ──────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("ban") & filters.user(Config.ADMIN))
async def do_ban(bot: Client, message: Message):
    parts  = message.text.split(None, 2)
    userid = parts[1] if len(parts) > 1 else None
    reason = parts[2] if len(parts) > 2 else "No reason provided"

    if not userid:
        return await message.reply_text(
            "╭━━━〔 🛡 BAN COMMAND 〕━━━╮\n"
            "┃  Usage: /ban [user_id] [reason]\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )

    text   = await message.reply_text("🧬 <i>Predator skill activating…</i>")
    result = await jishubotz.ban_user(userid)

    if result is True:
        await text.edit_text(
            "╭━━━〔 🛡 BARRIER ERECTED 〕━━━╮\n"
            f"┃  🆔  User    ·  <code>{userid}</code>\n"
            f"┃  ⚠️   Reason  ·  {reason}\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
            "Notify the traveler?",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📨 Send Alert", callback_data=f"sendAlert_{userid}_{reason}"),
                InlineKeyboardButton("🤫 Silent",     callback_data=f"noAlert_{userid}"),
            ]]),
        )
    else:
        await text.edit_text(
            "╭━━━〔 🛡 ALREADY BANNED 〕━━━╮\n"
            f"┃  <code>{userid}</code> is already restricted.\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )


# ── /unban ────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("unban") & filters.user(Config.ADMIN))
async def do_unban(bot: Client, message: Message):
    parts  = message.text.split(None, 1)
    userid = parts[1].strip() if len(parts) > 1 else None

    if not userid:
        return await message.reply_text(
            "╭━━━〔 🛡 UNBAN COMMAND 〕━━━╮\n"
            "┃  Usage: /unban [user_id]\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )

    text   = await message.reply_text("🌀 <i>Barrier dissolving…</i>")
    result = await jishubotz.is_unbanned(userid)

    if result is True:
        await text.edit_text(
            "╭━━━〔 ✨ BARRIER RELEASED 〕━━━╮\n"
            f"┃  <code>{userid}</code> access restored.\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
            "Notify the traveler?",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📨 Send Alert", callback_data=f"sendUnbanAlert_{userid}"),
                InlineKeyboardButton("🤫 Silent",     callback_data=f"NoUnbanAlert_{userid}"),
            ]]),
        )
    elif result is False:
        await text.edit_text(
            "╭━━━〔 🛡 NOT BANNED 〕━━━╮\n"
            f"┃  <code>{userid}</code> has no restriction.\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )
    else:
        await text.edit_text(RUI.error(f"Unban failed: <code>{result}</code>"))
