"""
plugins/file_rename.py
Rename pipeline — fully concurrent, correct filenames, premium-gated.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time

from pyrogram import Client, filters
from pyrogram.enums import MessageMediaType
from pyrogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ForceReply,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import Config
from helper.database import jishubotz
from helper.ffmpeg import (
    add_metadata,
    fix_thumb,
    get_duration_hachoir,
    run_blocking,
    take_screen_shot,
)
from helper.utils import add_prefix_suffix, convert, humanbytes, TimeFormatter
from messages import log, Msg

logger = logging.getLogger(__name__)

_VIDEO_EXTS = (
    ".mp4", ".mkv", ".avi", ".mov", ".webm",
    ".flv", ".ts",  ".m4v", ".wmv", ".3gp",
)
_CBZ_PDF_EXTS = (".cbz", ".pdf")


# ─────────────────────────────────────────────────────────────────────────────
# In-memory caches  (keyed by Telegram message_id)
# ─────────────────────────────────────────────────────────────────────────────
_pending:            dict[int, str]    = {}   # msg_id → exact user filename
_file_cache:         dict[int, object] = {}   # msg_id → original file Message
_upload_type_cache:  dict[int, str]    = {}   # msg_id → callback_data string
_manual_tasks:       dict[str, object] = {}   # job_id → asyncio.Task (manual pipeline)


# ══════════════════════════════════════════════════════════════════════════════
# Concurrency control
# ══════════════════════════════════════════════════════════════════════════════
_DEFAULT_GLOBAL_LIMIT:       int = 10
_DEFAULT_USER_LIMIT:         int = 3
_DEFAULT_TRANSMISSION_LIMIT: int = 4

_global_limit:       int = _DEFAULT_GLOBAL_LIMIT
_user_limit:         int = _DEFAULT_USER_LIMIT
_transmission_limit: int = _DEFAULT_TRANSMISSION_LIMIT

# Semaphore that gates every download+upload call.
# When all slots are taken the coroutine waits here instead of spawning
# another Pyrogram TCP connection — keeps Koyeb free tier alive.
_transmission_sem: asyncio.Semaphore = asyncio.Semaphore(_DEFAULT_TRANSMISSION_LIMIT)

_user_active:      dict[int, int] = {}
_user_active_lock: asyncio.Lock   = asyncio.Lock()


def get_transmission_sem() -> asyncio.Semaphore:
    """Return the live transmission semaphore (used by auto_rename too)."""
    return _transmission_sem


async def load_limits_from_db() -> None:
    """
    Load persisted limits from MongoDB on bot startup.
    Also rebuilds _transmission_sem so the value survives restarts.
    Call once from bot.py after the client starts.
    """
    global _global_limit, _user_limit, _transmission_limit, _transmission_sem
    try:
        g, u, _auto, t, _manual = await jishubotz.get_limits()
        _global_limit       = g
        _user_limit         = u
        _transmission_limit = t
        _transmission_sem   = asyncio.Semaphore(t)
        logger.info(
            f"[limits] Loaded from DB → global={g}, user={u}, "
            f"auto_daily={_auto}, transmission={t}, manual_daily={_manual}"
        )
    except Exception as e:
        logger.warning(f"[limits] Could not load from DB, using defaults: {e}")


def _active_jobs() -> int:
    return sum(_user_active.values())


async def _acquire_slot(user_id: int) -> bool:
    async with _user_active_lock:
        current = _user_active.get(user_id, 0)
        if current >= _user_limit:
            return False
        _user_active[user_id] = current + 1
        return True


async def _release_slot(user_id: int) -> None:
    async with _user_active_lock:
        count = _user_active.get(user_id, 0)
        _user_active[user_id] = max(0, count - 1)
        if _user_active[user_id] == 0:
            _user_active.pop(user_id, None)


# ══════════════════════════════════════════════════════════════════════════════
# Admin: /setlimit  /getlimit  /jobs
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("setlimit") & filters.user(Config.ADMIN))
async def cmd_setlimit(client: Client, message: Message):
    global _global_limit, _user_limit
    parts = message.text.strip().split()
    if len(parts) != 3:
        return await message.reply_text(
            "╭━━━〔 ⚙️ LIMIT SETTINGS 〕━━━╮\n"
            "┃  /setlimit global <n>        ·  concurrent rename slots\n"
            "┃  /setlimit user <n>          ·  per-user concurrent slots\n"
            "┃  /setlimit manual <n>        ·  manual rename daily cap (free)\n"
            "┃  /setlimit auto <n>          ·  auto-rename daily cap (free)\n"
            "┃  /setlimit transmission <n>  ·  Pyrogram transfer slots\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
            "<b>Example:</b> <code>/setlimit manual 5</code>"
        )
    scope = parts[1].lower()
    if scope not in ("global", "user", "auto", "transmission", "manual"):
        return await message.reply_text(
            "❌ Scope must be <code>global</code>, <code>user</code>, "
            "<code>auto</code>, <code>manual</code>, or <code>transmission</code>."
        )
    try:
        n = int(parts[2])
        if n < 1:
            raise ValueError
    except ValueError:
        return await message.reply_text("❌ Limit must be a positive integer.")

    if scope == "global":
        _global_limit = n
        await jishubotz.set_limits(global_limit=n)
        await message.reply_text(
            f"╭━━━〔 ⚙️ GLOBAL LIMIT UPDATED 〕━━━╮\n"
            f"┃  ⚡  Scope      ·  Global concurrent slots\n"
            f"┃  📊  New Limit  ·  <code>{n}</code>\n"
            f"┃  💾  Persisted  ·  ✅\n"
            f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )
    elif scope == "user":
        _user_limit = n
        await jishubotz.set_limits(user_limit=n)
        await message.reply_text(
            f"╭━━━〔 ⚙️ USER LIMIT UPDATED 〕━━━╮\n"
            f"┃  ⚡  Scope      ·  Per-user concurrent slots\n"
            f"┃  📊  New Limit  ·  <code>{n}</code>\n"
            f"┃  💾  Persisted  ·  ✅\n"
            f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )
    elif scope == "auto":
        await jishubotz.set_limits(auto_daily_limit=n)
        await message.reply_text(
            f"╭━━━〔 ⚙️ AUTO DAILY LIMIT UPDATED 〕━━━╮\n"
            f"┃  ⚡  Scope      ·  Auto-rename / day (free users)\n"
            f"┃  📊  New Limit  ·  <code>{n}</code> files/day\n"
            f"┃  💾  Persisted  ·  ✅\n"
            f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )
    elif scope == "manual":
        await jishubotz.set_limits(manual_daily_limit=n)
        await message.reply_text(
            f"╭━━━〔 ⚙️ MANUAL DAILY LIMIT UPDATED 〕━━━╮\n"
            f"┃  ⚡  Scope      ·  Manual rename / day (free users)\n"
            f"┃  📊  New Limit  ·  <code>{n}</code> files/day\n"
            f"┃  💾  Persisted  ·  ✅\n"
            f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )
    else:  # transmission
        global _transmission_limit, _transmission_sem
        _transmission_limit = n
        _transmission_sem   = asyncio.Semaphore(n)
        await jishubotz.set_limits(transmission_limit=n)
        await message.reply_text(
            f"╭━━━〔 ⚙️ TRANSMISSION LIMIT UPDATED 〕━━━╮\n"
            f"┃  ⚡  Scope      ·  Concurrent Pyrogram transfers\n"
            f"┃  📊  New Limit  ·  <code>{n}</code> simultaneous\n"
            f"┃  🔄  Live       ·  ✅ semaphore rebuilt instantly\n"
            f"┃  💾  Persisted  ·  ✅ survives restart\n"
            f"┃\n"
            f"┃  ℹ️  Files beyond this limit queue silently\n"
            f"┃     until a transfer slot frees up.\n"
            f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
            f"<i>Recommended for Koyeb free tier: 2–4</i>"
        )


@Client.on_message(filters.command("getlimit") & filters.user(Config.ADMIN))
async def cmd_getlimit(client: Client, message: Message):
    active       = _active_jobs()
    db_g, db_u, db_auto, db_t, db_manual = await jishubotz.get_limits()
    sem_waiting  = max(0, _transmission_limit - _transmission_sem._value)
    per_user_lines = "\n".join(
        f"┃  🆔  <code>{uid}</code>  ·  {cnt} job{'s' if cnt != 1 else ''}"
        for uid, cnt in _user_active.items()
    ) or "┃  └  <i>None</i>"
    in_sync = (db_g == _global_limit and db_u == _user_limit and db_t == _transmission_limit)
    await message.reply_text(
        f"╭━━━〔 ⚙️ LIMITS 〕━━━╮\n"
        f"┃  🌐  Global concurrent    ·  <code>{_global_limit}</code>\n"
        f"┃  👤  Per-user concurrent  ·  <code>{_user_limit}</code>\n"
        f"┃  ✏️  Manual rename/day    ·  <code>{db_manual}</code> (free users)\n"
        f"┃  🔄  Auto-rename/day      ·  <code>{db_auto}</code> (free users)\n"
        f"┃  📡  Transmission slots   ·  <code>{_transmission_limit}</code> "
        f"(<code>{sem_waiting}</code> in use)\n"
        f"┃  ⚡  Active rename jobs   ·  <code>{active}</code>\n"
        f"┃  💾  DB in sync           ·  {'✅' if in_sync else '⚠️ Mismatch'}\n"
        f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
        f"<b>Active per user:</b>\n{per_user_lines}"
    )


@Client.on_message(filters.command("jobs") & filters.user(Config.ADMIN))
async def cmd_jobs(client: Client, message: Message):
    # Collect manual rename jobs
    manual_jobs = dict(_user_active)
    
    # Collect auto rename jobs
    try:
        from plugins.auto_rename import _active_jobs as auto_jobs
        auto_by_user = {}
        for job in auto_jobs.values():
            uid = job.user_id
            auto_by_user[uid] = auto_by_user.get(uid, 0) + 1
    except Exception:
        auto_by_user = {}
    
    # Merge both
    all_jobs = {}
    for uid, cnt in manual_jobs.items():
        all_jobs[uid] = all_jobs.get(uid, 0) + cnt
    for uid, cnt in auto_by_user.items():
        all_jobs[uid] = all_jobs.get(uid, 0) + cnt
    
    total_active = sum(all_jobs.values())
    
    if not all_jobs:
        return await message.reply_text(
            "╭━━━〔 📋 ACTIVE JOBS 〕━━━╮\n"
            "┃  ✨  No active jobs right now.\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )
    
    per_user_lines = "\n".join(
        f"┃  👤  <code>{uid}</code>  ·  {cnt} job{'s' if cnt != 1 else ''}"
        for uid, cnt in sorted(all_jobs.items(), key=lambda x: -x[1])
    )
    
    await message.reply_text(
        f"╭━━━〔 📋 ACTIVE JOBS 〕━━━╮\n"
        f"┃  ⚡  Total  ·  <code>{total_active}</code> / <code>{_global_limit}</code>\n"
        f"┃  📊  Manual + Auto\n"
        f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
        f"<b>Per-user breakdown:</b>\n{per_user_lines}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — File received → premium gate → show action buttons
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & (filters.document | filters.audio | filters.video))
async def rename_start(client: Client, message: Message):
    file    = getattr(message, message.media.value)
    user_id = int(message.from_user.id)

    # ── Single round-trip: ban check + mode + premium ─────────────────────
    if await jishubotz.is_banned(user_id):
        return await message.reply(
            "╭━━━〔 🛡 ACCESS DENIED 〕━━━╮\n"
            "┃  ⛔  Your account is restricted.\n"
            "┃  Contact @naruto0927 to appeal.\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )

    # ── Auto-rename mode: NO premium gate — free users can use auto-rename ─
    rename_mode = await jishubotz.get_rename_mode(user_id)
    if rename_mode == "auto":
        from plugins.auto_rename import run_auto_rename
        return await run_auto_rename(client, message)

    # ── Manual rename: free users have a daily cap; premium = unlimited ─────
    _is_prem = await jishubotz.is_premium(user_id)
    if not _is_prem:
        _used_today  = await jishubotz.get_manual_rename_today(user_id)
        _day_limit   = await jishubotz.get_manual_daily_limit()
        if _used_today >= _day_limit:
            return await message.reply_text(
                f"╭━━━〔 ⚡ DAILY LIMIT REACHED 〕━━━╮\n"
                f"┃  📊  Used  ·  {_used_today} / {_day_limit} today\n"
                f"┃  ⏱   Resets at midnight UTC\n"
                f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
                f"Upgrade to <b>Tempest Elite</b> for unlimited renames 👑",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("👑 My Status", callback_data="check_premium_status"),
                ]])
            )

    # ── File size gate ────────────────────────────────────────────────────
    from helper.userbot import userbot_available
    is_large = file.file_size > Config.BOT_MAX_SIZE
    if is_large:
        # Premium already confirmed above — just check userbot availability
        if not userbot_available():
            return await message.reply_text(
                "╭━━━〔 ⚠️ BARRIER LIMIT 〕━━━╮\n"
                "┃  📦  File exceeds 2 GB.\n"
                "┃  ⚡  STRING_SESSION not configured.\n"
                "╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
                "Contact the admin to enable large-file support."
            )
        if file.file_size > Config.USER_MAX_SIZE:
            return await message.reply_text(
                "╭━━━〔 ⚠️ BARRIER LIMIT 〕━━━╮\n"
                "┃  📦  Exceeds 4 GB — maximum barrier.\n"
                "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
            )

    filename = file.file_name or ""
    ext      = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    _audio_exts = (".mp3", ".flac", ".aac", ".ogg", ".opus", ".wav", ".m4a", ".wma", ".aiff")
    is_cbz_pdf  = ext in ("cbz", "pdf")
    is_audio    = (message.media == MessageMediaType.AUDIO) or ext in _audio_exts
    is_video    = (message.media == MessageMediaType.VIDEO) or ext in [e.lstrip(".") for e in _VIDEO_EXTS]

    row1 = [InlineKeyboardButton("📄 Document", callback_data="upload_document")]
    if is_video:
        row1.append(InlineKeyboardButton("🎬 Video", callback_data="upload_video"))
    if is_audio:
        row1.append(InlineKeyboardButton("🎵 Audio", callback_data="upload_audio"))

    row2 = []
    if is_cbz_pdf:
        row2.append(InlineKeyboardButton("📚 CBZ/PDF", callback_data="upload_cbzpdf"))
    row2.append(InlineKeyboardButton("📊 MediaInfo", callback_data="action_mediainfo"))

    buttons = [row1, row2]
    if is_video:
        buttons.append([
            InlineKeyboardButton("📸 Grid",   callback_data="media_screenshot"),
            InlineKeyboardButton("🎞️ Sample", callback_data="media_sample"),
        ])

    # Steal Thumb available for all file types
    buttons.append([
        InlineKeyboardButton("🪄 Steal Thumb", callback_data="action_steal_thumb"),
    ])



    action_text = (
        f"╭━━━〔 📂 FILE ACQUIRED 〕━━━╮\n"
        f"┃  📛  Name  ·  <code>{(filename or 'Unknown')[:40]}</code>\n"
        f"┃  📦  Size  ·  {humanbytes(file.file_size)}\n"
        f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
        f"<i>⚡ Great Sage awaits your command.\n"
        f"Select an action below to begin.</i>"
    )
    pic = await jishubotz.get_pic("rename_pic")
    if pic:
        try:
            sent = await message.reply_photo(
                photo=pic,
                caption=action_text,
                reply_to_message_id=message.id,
                reply_markup=InlineKeyboardMarkup(buttons),
            )
            _file_cache[sent.id] = message
            return
        except Exception:
            pass

    sent = await message.reply(
        text=action_text,
        reply_to_message_id=message.id,
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    _file_cache[sent.id] = message


@Client.on_callback_query(filters.regex("^check_premium_status$"))
async def cb_check_premium(bot, update):
    is_prem = await jishubotz.is_premium(update.from_user.id)
    if is_prem:
        await update.answer("✦ Premium active ✓", show_alert=True)
    else:
        await update.answer("✦ No premium. Contact admin.", show_alert=True)



# ══════════════════════════════════════════════════════════════════════════════
# MediaInfo button
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex("^action_mediainfo$"))
async def cb_mediainfo(bot, update):
    await update.answer("📊 Generating MediaInfo...")
    asyncio.create_task(_handle_mediainfo(bot, update))


async def _handle_mediainfo(bot, update) -> None:
    from plugins.mediainfo import (
        _ffprobe_sync,
        _build_telegraph_nodes,
        _build_plain_fallback,
        _partial_download      as _mi_partial_dl,
        _upload_to_telegraph   as _telegraph_upload,
    )

    chat_id      = update.message.chat.id
    file_message = update.message.reply_to_message

    if not file_message or not file_message.media:
        return await update.message.edit("❌ <b>File not found.</b>")

    media     = getattr(file_message, file_message.media.value, None)
    raw_name  = getattr(media, "file_name", None) or f"file_{int(time.time())}"
    file_size = getattr(media, "file_size", 0)

    ms = await update.message.edit("⏳ <b>Status:</b> <code>[▒▒▒▒▒▒▒]</code> Fetching Source...")

    user_id  = update.from_user.id
    job_id   = f"mi_{user_id}_{int(time.time() * 1000)}"
    dl_dir   = f"downloads/{job_id}"
    os.makedirs(dl_dir, exist_ok=True)

    safe_name = "".join(c for c in raw_name if c.isalnum() or c in "._- []@")
    file_path = os.path.join(dl_dir, safe_name)

    try:
        partial_limit = min(int(file_size * 0.15), 50 * 1024 * 1024)
        partial_limit = max(partial_limit, 2 * 1024 * 1024)
        await _safe_edit(ms, f"⏳ <b>Fetching header</b>  {humanbytes(partial_limit)} / {humanbytes(file_size)}")

        file_path = await _mi_partial_dl(bot, file_message, file_path, partial_limit)

        if not file_path or not os.path.exists(file_path):
            return await _safe_edit(ms, "❌ <b>Internal Error</b>\nDownload failed. Please try again.")

        await _safe_edit(ms, "⚙️ <b>Status:</b> <code>[●●●●○○○]</code> Analysing Streams...")
        data = await run_blocking(_ffprobe_sync, file_path)

        await _safe_edit(ms, "📤 <b>Status:</b> <code>[███████]</code> Publishing Report...")
        bot_username = getattr(Config, "BOT_USERNAME", "YourBot")
        nodes    = _build_telegraph_nodes(data, raw_name, file_size, bot_username)
        page_url = await _telegraph_upload(f"MediaInfo of {raw_name}", nodes, bot_username)

        if page_url:
            await ms.edit(
                f"◈ <b>MediaInfo Analysis</b>\n"
                f"<blockquote><code>{raw_name}</code> ({humanbytes(file_size)})</blockquote>\n\n"
                f"✅ <b>Streams Analysed Successfully.</b>\n"
                f"📊 <b>Report:</b> {page_url}",
                disable_web_page_preview=False,
            )
        else:
            # Telegraph failed — send plain text fallback inline
            plain   = _build_plain_fallback(data, raw_name, file_size)
            snippet = plain[:3800] + ("\n\n… (truncated)" if len(plain) > 3800 else "")
            await ms.edit(f"✦ <b>MediaInfo</b>\n\n<code>{snippet}</code>")

    except Exception as e:
        logger.error("MediaInfo error: %s", e)
        await _safe_edit(ms, f"❌ <b>Internal Error</b>\nMediaInfo failed: <code>{e}</code>")
    finally:
        _cleanup_dir(dl_dir)


# ══════════════════════════════════════════════════════════════════════════════
# Screenshot button
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex("^media_screenshot$"))
async def cb_screenshot(bot, update):
    await update.answer("📸 Generating screenshots...")
    asyncio.create_task(_handle_screenshot(bot, update))


async def _handle_screenshot(bot, update) -> None:
    from helper.ffmpeg import generate_screenshot_grid
    from plugins.mediainfo import _partial_download as _mi_partial_dl

    chat_id      = update.message.chat.id
    file_message = update.message.reply_to_message

    if not file_message or not file_message.media:
        return await update.message.edit("❌ <b>File not found.</b>")

    ms      = await update.message.edit("⏳ <b>Status:</b> <code>[▒▒▒▒▒▒▒]</code> Fetching Source...")
    user_id = update.from_user.id
    job_id  = f"ss_{user_id}_{int(time.time() * 1000)}"
    dl_dir  = f"downloads/{job_id}"
    os.makedirs(dl_dir, exist_ok=True)

    file     = getattr(file_message, file_message.media.value)
    filename = file.file_name or "video.mkv"
    dl_path  = f"{dl_dir}/{filename}"

    try:
        # Full download required — ffmpeg needs to seek to 20/40/60/80/95%
        # of the video duration for an evenly-spaced grid. Partial downloads
        # only contain early timestamps (everything comes out 00:00:xx).
        await _safe_edit(ms, "╭━━━〔 💠 RIMURU SYSTEM 〕━━━╮\n┃  ⬇️  Acquiring file data...\n┃  <code>[░░░░░░░░░░]</code>\n╰━━━━━━━━━━━━━━━━━━━━━━━━╯")
        try:
            await bot.download_media(message=file_message, file_name=dl_path)
        except Exception as e:
            return await _safe_edit(ms, f"╭━━━〔 ❌ SKILL FAILED 〕━━━╮\n┃  ⬇️  Download error:\n┃  <code>{e}</code>\n╰━━━━━━━━━━━━━━━━━━━━━━━━╯")
        if not os.path.exists(dl_path) or os.path.getsize(dl_path) == 0:
            return await _safe_edit(ms, "❌ <b>Download failed.</b>")

        await _safe_edit(ms, "⚙️ <b>Status:</b> <code>[●●●●●○○]</code> Generating Grid...")

        ss_count  = await jishubotz.get_screenshot_count(update.from_user.id)
        # cols: 1 → 1, 2-3 → 2, 4+ → 3, 8+ → 4
        if ss_count <= 1:
            cols = 1
        elif ss_count <= 3:
            cols = 2
        elif ss_count <= 9:
            cols = 3
        else:
            cols = 4
        grid_path = await generate_screenshot_grid(dl_path, dl_dir, count=ss_count, cols=cols)

        if not grid_path:
            return await _safe_edit(ms, "❌ <b>Error:</b> Invalid video stream or codec.")

        await ms.delete()
        await bot.send_photo(
            chat_id,
            photo=grid_path,
            caption=f"◈ <b>Video Preview Grid</b>\n<blockquote><code>{filename}</code></blockquote>\n\n📸 <b>Layout:</b> {ss_count} Frames generated.",
        )

    except Exception as e:
        logger.error("Screenshot error: %s", e)
        await _safe_edit(ms, f"❌ <b>Internal Error</b>\nScreenshot failed: <code>{e}</code>")
    finally:
        _cleanup_dir(dl_dir)


# ══════════════════════════════════════════════════════════════════════════════
# Sample Video button
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex("^media_sample$"))
async def cb_sample_video(bot, update):
    await update.answer("🎬 Generating sample clip...")
    asyncio.create_task(_handle_sample_video(bot, update))


async def _handle_sample_video(bot, update) -> None:
    from helper.ffmpeg import generate_sample_video, get_video_duration
    from plugins.mediainfo import _partial_download as _mi_partial_dl

    chat_id      = update.message.chat.id
    file_message = update.message.reply_to_message

    if not file_message or not file_message.media:
        return await update.message.edit("❌ <b>File not found.</b>")

    ms      = await update.message.edit("⏳ <b>Fetching file header…</b>")
    user_id = update.from_user.id
    job_id  = f"smp_{user_id}_{int(time.time() * 1000)}"
    dl_dir  = f"downloads/{job_id}"
    os.makedirs(dl_dir, exist_ok=True)

    file        = getattr(file_message, file_message.media.value)
    filename    = file.file_name or "video.mkv"
    file_size   = getattr(file, "file_size", 0) or 0
    dl_path     = f"{dl_dir}/{filename}"
    sample_path = None

    # Strategy: download a partial chunk around the target sample window.
    # Step 1 — fetch first 5 MB to read container duration from the header.
    # Step 2 — compute the start offset (10–70% of duration).
    # Step 3 — for the sample window (30s at typical 2–4 Mbps ≈ 8–15 MB),
    #           download only that byte range via stream_media offset+limit.
    #           This means we download ~20 MB instead of 1–2 GB.
    _HEADER_BYTES = 5 * 1024 * 1024
    _SAMPLE_BYTES = 50 * 1024 * 1024   # 50 MB window — covers 30s at high bitrate

    try:
        # ── Phase 1: fetch header to read duration ────────────────────────────
        result = await _mi_partial_dl(bot, file_message, dl_path, _HEADER_BYTES)
        if not result:
            return await _safe_edit(ms, "❌ <b>Download failed.</b>")

        total_duration = await get_video_duration(dl_path)

        # ── Phase 2: compute sample start byte offset ─────────────────────────
        if total_duration > 0 and file_size > 0 and total_duration > 30:
            import random as _random
            lo    = total_duration * 0.10
            hi    = max(total_duration * 0.70, lo + 1.0)
            start_sec = _random.uniform(lo, hi)
            start_sec = min(start_sec, total_duration - 30.5)
            # Byte offset proportional to start time
            byte_offset = int((start_sec / total_duration) * file_size)
            byte_offset = max(0, byte_offset - 2 * 1024 * 1024)  # 2 MB before target
        else:
            byte_offset = 0

        # ── Phase 3: fetch just the sample window into a SEPARATE file ──────
        # IMPORTANT: do NOT append to dl_path. The header (Phase 1) already
        # lives there. Appending the window produces a non-contiguous byte
        # stream; ffmpeg cannot seek it and sample generation fails.
        # Instead write the window to its own temp file.
        await _safe_edit(ms, "⏳ <b>Status:</b> <code>[▒▒▒▒░░░]</code> Fetching Window...")
        window_path = f"{dl_dir}/window_{int(time.time())}.tmp"
        window_ok   = False
        try:
            written = 0
            with open(window_path, "wb") as wf:
                async for chunk in bot.stream_media(
                    file_message,
                    offset=byte_offset // (1024 * 1024),  # offset in MB (Pyrogram unit)
                    limit=_SAMPLE_BYTES,
                ):
                    wf.write(chunk)
                    written += len(chunk)
                    if written >= _SAMPLE_BYTES:
                        break
            if written > 0:
                window_ok = True
        except Exception:
            pass  # fall back to header-only path below

        # Always use the full file for sample generation.
        # Window files (partial downloads) may not have valid frame headers at the start,
        # causing ffmpeg to fail. Full file is safer and ffmpeg can seek within it.
        source_for_sample = dl_path

        await _safe_edit(ms, "⚙️ <b>Status:</b> <code>[●●●●●○○]</code> Trimming Clip...")

        sample_dur  = await jishubotz.get_sample_duration(update.from_user.id)
        sample_path = await generate_sample_video(source_for_sample, dl_dir, duration=sample_dur)

        if not sample_path:
            return await _safe_edit(ms, "❌ <b>Error:</b> Could not trim file. Check format.")

        await ms.delete()
        await bot.send_video(
            chat_id,
            video=sample_path,
            caption=f"◈ <b>Sample Generated</b>\n<blockquote><code>{filename}</code></blockquote>\n\n🎞️ <b>Duration:</b> {sample_dur} seconds.",
            supports_streaming=True,
        )

    except Exception as e:
        logger.error("Sample video error: %s", e)
        await _safe_edit(ms, f"❌ <b>Internal Error</b>\nSample failed: <code>{e}</code>")
    finally:
        if sample_path:
            _safe_remove(sample_path)
        # Clean up window temp file if it was created
        try:
            if "window_path" in dir() and window_path and os.path.exists(window_path):
                os.remove(window_path)
        except Exception:
            pass
        _cleanup_dir(dl_dir)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Rename button tapped → ask for filename
# ══════════════════════════════════════════════════════════════════════════════


@Client.on_callback_query(filters.regex(r"^manual_cancel_(.+)$"))
async def cb_manual_cancel(bot, update):
    """Cancel a running manual rename pipeline."""
    job_id = update.data[len("manual_cancel_"):]
    task   = _manual_tasks.get(job_id)

    if task and not task.done():
        task.cancel()
        await update.answer("🗑  Task cancelled.", show_alert=True)
        try:
            await update.message.edit(
                f"╭━━━〔 🗑 CANCELLED 〕━━━╮\n"
                f"┃  ⚡  Rename task removed.\n"
                f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
            )
        except Exception:
            pass
    else:
        await update.answer("⚠️  Task already finished or not found.", show_alert=True)

    _manual_tasks.pop(job_id, None)


@Client.on_callback_query(filters.regex("^upload_"))
async def ask_filename(bot, update):
    await update.answer()
    file_message = update.message.reply_to_message
    if not file_message or not file_message.media:
        return await update.message.edit("❌ <b>Internal Error</b>\nFile not found or expired.")

    file     = getattr(file_message, file_message.media.value)
    filename = file.file_name or "file"

    await update.message.delete()

    sent = await bot.send_message(
        update.message.chat.id,
        text=(
            f"◈ <b>Rename File</b>\n"
            f"<blockquote><b>Old:</b> <code>{filename}</code></blockquote>\n\n"
            f"➜ <i>Please send the <b>New Filename</b> now.</i>\n"
            f"➜ Use /cancel to abort."
        ),
        reply_markup=ForceReply(True),
    )

    _upload_type_cache[sent.id] = update.data
    _file_cache[sent.id]        = file_message


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — User typed filename → show confirm button
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.reply)
async def refunc(client: Client, message: Message):
    reply_message = message.reply_to_message
    if not (reply_message.reply_markup and isinstance(reply_message.reply_markup, ForceReply)):
        return

    upload_type_stored = _upload_type_cache.get(reply_message.id)
    file_message       = _file_cache.get(reply_message.id)

    if not upload_type_stored or not file_message:
        return

    new_name = message.text.strip()

    await message.delete()
    await reply_message.delete()

    _upload_type_cache.pop(reply_message.id, None)
    _file_cache.pop(reply_message.id, None)

    media = getattr(file_message, file_message.media.value)

    if "." not in new_name:
        extn = (
            media.file_name.rsplit(".", 1)[-1]
            if "." in (media.file_name or "")
            else "mkv"
        )
        new_name = f"{new_name}.{extn}"

    sent = await message.reply(
        text=f"◈ <b>Final Review</b>\n<blockquote><b>New:</b> <code>{new_name}</code></blockquote>\n\n➜ <i>Proceed with this name?</i>",
        reply_to_message_id=file_message.id,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_{upload_type_stored}")
        ]]),
    )

    _pending[sent.id] = new_name


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — Confirm → acquire slot → fire task (non-blocking)
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex("^confirm_upload_"))
async def doc(bot, update):
    user_id = update.from_user.id

    if not await jishubotz.is_premium(user_id):
        return await update.answer(
            "⛔ <b>Access Denied</b>\nYour premium has expired. Contact the admin to renew.",
            show_alert=True,
        )

    acquired = await _acquire_slot(user_id)
    if not acquired:
        # Silently dismiss the button tap — user already has max concurrent jobs.
        # No noisy popup; the existing progress messages show what's running.
        return await update.answer(
            f"⏳ {_user_limit} job(s) already running — new file queued when a slot frees up.",
            show_alert=False,
        )

    # Snapshot thumbnail NOW at confirm time — locks it for this job
    # so mid-rename thumb changes don't bleed into this pipeline run
    try:
        _locked_thumb = await jishubotz.get_thumbnail(user_id)
    except Exception:
        _locked_thumb = None

    # Fire immediately — never awaited sequentially
    task = asyncio.create_task(_run_rename(bot, update, locked_thumb_id=_locked_thumb))

    # Store task reference for direct cancel support
    # We need the job_id first — read it from _pending via the message id
    # job_id is set inside _pipeline after registration; we store the task ref there


# ══════════════════════════════════════════════════════════════════════════════
# Rename job wrapper — owns semaphore + slot lifecycle
# ══════════════════════════════════════════════════════════════════════════════

async def _run_rename(bot, update, locked_thumb_id: str | None = None) -> None:
    """
    Parallel execution:
    - asyncio.create_task() fires this immediately
    - _global_sem wraps ONLY the upload step
    - Downloads run fully in parallel across all tasks
    """
    user_id = update.from_user.id
    try:
        await _pipeline(bot, update, user_id, update.message.chat.id, locked_thumb_id=locked_thumb_id)
    except asyncio.CancelledError:
        logger.info("Task cancelled for user=%s", user_id)
    except Exception as e:
        logger.exception("Unhandled error in rename task user=%s: %s", user_id, e)
    finally:
        await _release_slot(user_id)


# ══════════════════════════════════════════════════════════════════════════════
# Core pipeline: download → (metadata) → upload
# ══════════════════════════════════════════════════════════════════════════════

async def _pipeline(bot, update, user_id: int, chat_id: int, locked_thumb_id: str | None = None) -> None:
    os.makedirs("Metadata", exist_ok=True)

    upload_type = update.data.split("_")[-1]

    new_filename_raw = _pending.pop(update.message.id, None)
    if not new_filename_raw:
        return await update.message.edit(
            "╭━━━〔 ❌ SYSTEM ERROR 〕━━━╮\n┃  Great Sage: Metadata unreadable.\n╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )

    # ── ONE round-trip: load all pipeline settings ───────────────────────
    _ps = await jishubotz.get_pipeline_settings(chat_id)
    prefix = _ps["prefix"]
    suffix = _ps["suffix"]
    try:
        new_filename = add_prefix_suffix(new_filename_raw, prefix, suffix)
    except Exception as e:
        return await update.message.edit(
            f"❌ <b>Internal Error</b>\nPrefix/Suffix failed: <code>{e}</code>"
        )

    job_id       = f"{user_id}_{int(time.time() * 1000)}"
    dl_dir       = f"downloads/{job_id}"
    os.makedirs(dl_dir, exist_ok=True)

    ext           = new_filename.rsplit(".", 1)[-1] if "." in new_filename else "mkv"
    file_path     = f"{dl_dir}/_tmp_{job_id}.{ext}"
    metadata_path = f"Metadata/{job_id}.{ext}"

    file           = update.message.reply_to_message
    ph_path        = None
    _bool_metadata = False

    logger.info(
        "▶ PIPELINE START  user=%s  filename=%s  job=%s  active_total=%s",
        user_id, new_filename, job_id, _active_jobs()
    )

    try:
        try:
            ms = await update.message.edit("╭━━━〔 💠 RIMURU SYSTEM 〕━━━╮\n┃  ⬇️  Acquiring file data...\n┃  <code>[░░░░░░░░░░]</code>\n╰━━━━━━━━━━━━━━━━━━━━━━━━╯")
        except Exception:
            ms = update.message

        # Cancel button — stays on the message throughout the job
        cancel_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"🗑 Cancel  [{job_id}]", callback_data=f"manual_cancel_{job_id}")
        ]])
        try:
            await ms.edit(
                "╭━━━〔 💠 RIMURU SYSTEM 〕━━━╮\n"
                "┃  ⬇️  Acquiring file data...\n"
                "┃  <code>[░░░░░░░░░░]</code>\n"
                "╰━━━━━━━━━━━━━━━━━━━━━━━━╯",
                reply_markup=cancel_kb,
            )
        except Exception:
            pass

        # Register this task so the cancel callback can reach it
        _manual_tasks[job_id] = asyncio.current_task()

        # ── Choose client: userbot for >2 GB, bot otherwise ───────────────────
        # file = update.message.reply_to_message (the original file message)
        from helper.userbot import get_userbot, userbot_available
        try:
            _media_obj = getattr(file, file.media.value) if file.media else None
            _file_size = getattr(_media_obj, "file_size", 0) or 0
        except Exception:
            _file_size = 0

        _large     = _file_size > Config.BOT_MAX_SIZE
        _dl_client = bot
        _ul_client = bot

        if _large and userbot_available():
            _ub = await get_userbot()
            if _ub:
                _dl_client = _ub
                _ul_client = _ub
                logger.info("[pipeline] job=%s using userbot for %s MB file",
                            job_id, _file_size // 1024 // 1024)
            else:
                logger.warning("[pipeline] job=%s userbot unavailable — bot limit applies", job_id)

        # ── Download — gated by transmission semaphore ───────────────────────
        # When all slots are busy, the coroutine waits here (queued) instead
        # of opening yet another Pyrogram TCP connection.
        async with _transmission_sem:
            try:
                await _dl_client.download_media(
                    message=file,
                    file_name=file_path,
                    progress=_pipeline_progress,
                    progress_args=(job_id, "Downloading", ms, time.time(), cancel_kb),
                )
            except asyncio.CancelledError:
                await _safe_edit(ms, "🛑 <b>Status:</b> <code>[✖]</code> Session Cancelled.")
                return
            except Exception as e:
                return await _safe_edit(ms, f"╭━━━〔 ❌ SKILL FAILED 〕━━━╮\n┃  ⬇️  Download error:\n┃  <code>{e}</code>\n╰━━━━━━━━━━━━━━━━━━━━━━━━╯")

        duration = await get_duration_hachoir(file_path)

        # ── Thumbnail ─────────────────────────────────────────────────────────
        # locked_thumb_id was snapshotted at confirm time — immune to
        # mid-rename thumb changes by the user.
        media   = getattr(file, file.media.value)
        # locked_thumb_id (from confirm callback) takes priority;
        # fall back to the pre-loaded settings thumbnail
        c_thumb = locked_thumb_id or _ps["thumbnail"]

        if c_thumb:
            try:
                dl = await bot.download_media(c_thumb)
                if dl and os.path.exists(dl) and os.path.getsize(dl) > 0:
                    _, __, ph_path = await fix_thumb(dl)
                else:
                    if dl and os.path.exists(dl):
                        os.remove(dl)
                    ph_path = None
            except Exception as e:
                logger.warning("Custom thumbnail download failed (%s) — using auto thumb", e)
                await jishubotz.set_thumbnail(chat_id, file_id=None)
                ph_path = None

        if ph_path is None and media.thumbs:
            try:
                ph_path_ = await take_screen_shot(
                    file_path, dl_dir,
                    random.randint(0, max(duration - 1, 0)),
                )
                if ph_path_ and os.path.exists(ph_path_) and os.path.getsize(ph_path_) > 0:
                    _, __, ph_path = await fix_thumb(ph_path_)
            except Exception as e:
                ph_path = None
                logger.warning("Auto thumbnail error: %s", e)

        # ── Caption (from pre-loaded _ps) ─────────────────────────────────────
        c_caption = _ps["caption"]
        if c_caption:
            try:
                caption = c_caption.format(
                    filename=new_filename,
                    filesize=humanbytes(media.file_size),
                    duration=convert(duration),
                )
            except Exception as e:
                return await _safe_edit(ms, f"❌ <b>Caption error:</b> <code>{e}</code>")
        else:
            caption = f"**{new_filename}**"

        # ── Metadata ──────────────────────────────────────────────────────────
        is_cbz_pdf_upload = upload_type == "cbzpdf"

        if not is_cbz_pdf_upload:
            _bool_metadata  = _ps["metadata"]
            metadata_fields = _ps["metadata_fields"]
            if _bool_metadata:
                _has_meta_vals = any((v or "").strip() for v in metadata_fields.values())
                if _has_meta_vals:
                    result = await add_metadata(file_path, metadata_path, metadata_fields, ms)
                    if not result:
                        _bool_metadata = False
                else:
                    _bool_metadata = False
            else:
                await _safe_edit(ms, "╭━━━〔 💠 RIMURU SYSTEM 〕━━━╮\n┃  🧬  Evolving metadata...\n╰━━━━━━━━━━━━━━━━━━━━━━━━╯", cancel_kb)
        else:
            await _safe_edit(ms, "📚 <b>Preparing upload…</b>")

        # ── Upload — gated by same transmission semaphore as download ──────────
        upload_path = metadata_path if _bool_metadata else file_path

        await _safe_edit(ms, "╭━━━〔 💠 RIMURU SYSTEM 〕━━━╮\n┃  ⬆️  Transmitting...\n┃  <code>[██████████]</code>\n╰━━━━━━━━━━━━━━━━━━━━━━━━╯", cancel_kb)

        sent_message = None
        try:
            async with _transmission_sem:
                if upload_type in ("document", "cbzpdf"):
                    sent_message = await _ul_client.send_document(
                        chat_id,
                        document=upload_path,
                        file_name=new_filename,
                        thumb=ph_path,
                        caption=caption,
                        progress=_pipeline_progress,
                        progress_args=(job_id, "Uploading", ms, time.time(), cancel_kb),
                    )
                elif upload_type == "video":
                    sent_message = await _ul_client.send_video(
                        chat_id,
                        video=upload_path,
                        file_name=new_filename,
                        caption=caption,
                        thumb=ph_path,
                        duration=duration,
                        progress=_pipeline_progress,
                        progress_args=(job_id, "Uploading", ms, time.time(), cancel_kb),
                    )
                elif upload_type == "audio":
                    sent_message = await _ul_client.send_audio(
                        chat_id,
                        audio=upload_path,
                        file_name=new_filename,
                        caption=caption,
                        thumb=ph_path,
                        duration=duration,
                        progress=_pipeline_progress,
                        progress_args=(job_id, "Uploading", ms, time.time(), cancel_kb),
                    )

                if sent_message:
                    # ── Log to BIN/LOG channel with caption + thumb ───────
                    _orig_name = getattr(media, "file_name", "") or ""
                    _log_cap = (
                        f"📂 <b>{_orig_name}</b>\n➜ ✏️ <b>{new_filename}</b>"
                        if _orig_name else
                        f"✏️ <b>{new_filename}</b>"
                    )
                    try:
                        await bot.copy_message(
                            chat_id=Config.BIN_CHANNEL,
                            from_chat_id=chat_id,
                            message_id=sent_message.id,
                            caption=_log_cap,
                        )
                    except Exception as _le:
                        logger.warning("BIN_CHANNEL log failed: %s", _le)

        except asyncio.CancelledError:
            await _safe_edit(ms, "🛑 <b>Cancelled.</b>")
            return
        except Exception as e:
            return await _safe_edit(ms, f"❌ <b>Internal Error</b>\nUpload failed: <code>{e}</code>")

        if sent_message:
            if _ps.get("dump_mode") and _ps.get("dump_channel"):
                _orig_name = getattr(media, "file_name", "") or ""
                asyncio.create_task(
                    _dump_to_channel(bot, user_id, int(_ps["dump_channel"]), sent_message,
                                     original_name=_orig_name, new_name=new_filename)
                )

        # ── Increment manual daily counter (free limit tracking) ─────────────
        if not _ps.get("premium"):
            asyncio.create_task(jishubotz.inc_manual_rename_today(user_id))

        try:
            from plugins.leaderboard import record_rename, record_history
            display   = update.from_user.first_name if hasattr(update, "from_user") else str(user_id)
            file_sz   = getattr(media, "file_size", 0) or 0
            asyncio.create_task(record_rename(user_id, display))
            asyncio.create_task(record_history(user_id, new_filename, file_sz))
        except Exception:
            pass

        await ms.delete()
        logger.info(
            "✔ PIPELINE DONE   user=%s  filename=%s  job=%s",
            user_id, new_filename, job_id
        )

    finally:
        _manual_tasks.pop(job_id, None)
        if ph_path:
            _safe_remove(ph_path)
        if _bool_metadata:
            _safe_remove(metadata_path)
        _cleanup_dir(dl_dir)


# ══════════════════════════════════════════════════════════════════════════════
# Progress callback — feeds UI bar AND /status tracker, checks cancel
# ══════════════════════════════════════════════════════════════════════════════

# Per-message last-edit timestamps for throttling
_msg_last_edit: dict[int, float] = {}


async def _pipeline_progress(current: int, total: int, job_id: str, status_label: str, ms, _start, reply_markup=None):
    import math
    from helper.utils import humanbytes, TimeFormatter

    now  = time.time()
    diff = now - _start
    if diff < 0.5:
        return

    speed = current / diff if diff > 0 else 0
    eta_s = (total - current) / speed if speed > 0 else 0

    msg_key = getattr(ms, "id", id(ms))
    last    = _msg_last_edit.get(msg_key, 0)
    if current != total and (now - last) < 8:
        return
    _msg_last_edit[msg_key] = now

    try:
        pct       = current * 100 / total if total > 0 else 0
        filled    = math.floor(pct / 10)
        bar       = "█" * filled + "░" * (10 - filled)
        eta_str   = TimeFormatter(milliseconds=int(eta_s * 1000)) or "0s"
        phase     = "⬇️  Acquiring" if status_label == "Downloading" else "⬆️  Transmitting"
        size_str  = f"{humanbytes(current)} / {humanbytes(total)}"
        speed_str = humanbytes(speed)

        text = (
            f"╭━━━〔 💠 RIMURU SYSTEM 〕━━━╮\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃  {phase}\n"
            f"┃  <code>[{bar}]</code>  {round(pct, 1)}%\n"
            f"┃  📦  {size_str}\n"
            f"┃  ⚡  {speed_str}/s  ·  ⏱ {eta_str}\n"
            f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )

        await ms.edit(text, reply_markup=reply_markup)
    except Exception:
        pass
    finally:
        if current == total:
            _msg_last_edit.pop(msg_key, None)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

async def _safe_edit(ms, text: str, reply_markup=None) -> None:
    try:
        await ms.edit(text, reply_markup=reply_markup)
    except Exception:
        pass


def _safe_remove(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def _cleanup_dir(directory: str) -> None:
    try:
        if not os.path.exists(directory):
            return
        for f in os.listdir(directory):
            _safe_remove(os.path.join(directory, f))
        os.rmdir(directory)
    except Exception:
        pass


async def _dump_to_channel(
    bot,
    user_id: int,
    channel_id: int,
    sent_message,
    original_name: str = "",
    new_name: str = "",
) -> None:
    """Forward the renamed file to the user's dump channel with no caption."""
    try:
        await bot.copy_message(
            chat_id=channel_id,
            from_chat_id=sent_message.chat.id,
            message_id=sent_message.id,
        )
        logger.info("Dumped to channel %s for user %s", channel_id, user_id)
    except Exception as e:
        logger.error("Dump failed for user %s → channel %s: %s", user_id, channel_id, e)
        try:
            await bot.send_message(
                user_id,
                f"⚠️ Could not dump to channel `{channel_id}`: `{e}`",
                disable_notification=True,
            )
        except Exception:
            pass


def _fmt_dur(ms: int) -> str:
    s, ms = divmod(ms, 1000)
    m, s  = divmod(s, 60)
    h, m  = divmod(m, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    return f"{m}m {s:02d}s"


def _fmt_br(br) -> str:
    try:
        br = int(br)
        if br >= 1_000_000:
            return f"{br / 1_000_000:.2f} Mbps"
        return f"{br / 1_000:.0f} Kbps"
    except Exception:
        return str(br)
