"""
plugins/auto_rename.py
══════════════════════════════════════════════════════════════════════════════
Features
────────
• /mode          – toggle Manual ↔ Auto Rename
• /autorename    – set / view the naming template
• /setsource     – choose metadata extraction source
                   (filename | caption | both — "both" tries caption first)
• /setmedia      – preferred output container (document / video / audio)
• /autoqueue     – live view of the user's queue; cancel individual jobs
• Queue system   – 4 concurrent workers; every extra file waits
• Rename limits  – normal users: 30 renames/day  |  premium: unlimited
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from pyrogram import Client, filters
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import Config
from helper.database import jishubotz
from helper.ffmpeg import add_metadata, get_duration_hachoir
from helper.utils import add_prefix_suffix, convert, humanbytes

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Constants
# ══════════════════════════════════════════════════════════════════════════════

CONCURRENT_JOBS    = 4
# Auto-rename daily limit is now DB-driven via /setlimit auto <n>  (default 30)
_PROGRESS_THROTTLE = 4   # seconds between progress edits

# ══════════════════════════════════════════════════════════════════════════════
# Queue data structures
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class _Job:
    job_id:     str
    user_id:    int
    message:    Message
    status:     str = "queued"
    status_msg: Optional[object] = None
    accept_msg: Optional[object] = None   # queued-card shown before pipeline starts
    task:       Optional[asyncio.Task] = None
    queued_at:  float = field(default_factory=time.time)

    def short_name(self) -> str:
        try:
            m = self.message
            if m.document: return m.document.file_name or "document"
            if m.video:    return m.video.file_name    or "video"
            if m.audio:    return m.audio.file_name    or "audio"
        except Exception:
            pass
        return f"job-{self.job_id}"


_active_jobs:   dict[str, _Job] = {}
_waiting_queue: deque[_Job]     = deque()
_queue_lock     = asyncio.Lock()
_user_jobs:     dict[int, set[str]] = {}
_job_counter    = 0
_job_counter_lock = asyncio.Lock()


async def _new_job_id() -> str:
    """Thread-safe monotonic job ID.  Prefix 'ar' + zero-padded counter."""
    global _job_counter
    async with _job_counter_lock:
        _job_counter += 1
        return f"ar{_job_counter:06d}"


def _trim(s: str, n: int) -> str:
    return s if len(s) <= n else s[:n - 1] + "…"


# ══════════════════════════════════════════════════════════════════════════════
# Daily rename counter
# ══════════════════════════════════════════════════════════════════════════════

async def _check_daily(user_id: int):
    """Check if user can add another auto-rename job (does NOT increment — do that at pipeline start).
    Returns (allowed, used_today, limit). limit=0 means unlimited (premium)."""
    is_prem = await jishubotz.is_premium(user_id)
    if is_prem:
        return True, 0, 0
    used  = await jishubotz.get_auto_rename_today(user_id)
    limit = await jishubotz.get_auto_daily_limit()
    if used >= limit:
        return False, used, limit
    return True, used, limit


async def _inc_daily(user_id: int) -> None:
    """Increment the daily auto-rename counter. Call exactly once per job when it actually starts."""
    await jishubotz.inc_auto_rename_today(user_id)


# ══════════════════════════════════════════════════════════════════════════════
# /mode
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command("mode"))
async def cmd_mode(client: Client, message: Message):
    user_id = message.from_user.id
    if await jishubotz.is_banned(user_id):
        return await message.reply("⛔ <b>Access Denied</b>")
    if not await jishubotz.is_premium(user_id):
        return await message.reply_text(
            "◈ <b>Premium Required</b>\n\n"
            "<blockquote>This feature requires a premium plan.</blockquote>\n\n"
            "➤  Contact @naruto0927 to upgrade"
        )
    current = await jishubotz.get_rename_mode(user_id)
    await message.reply_text(_mode_text(current), reply_markup=_mode_keyboard(current))


@Client.on_callback_query(filters.regex(r"^set_mode_(manual|auto)$"))
async def cb_set_mode(client: Client, update: CallbackQuery):
    user_id  = update.from_user.id
    new_mode = update.data.split("_")[-1]
    if not await jishubotz.is_premium(user_id):
        return await update.answer("💎 Premium required.", show_alert=True)
    await jishubotz.set_rename_mode(user_id, new_mode)
    label = "Manual" if new_mode == "manual" else "Auto Rename"
    await update.answer(f"⚡ Mode set to {label} — Great Sage confirms.", show_alert=True)
    try:
        await update.message.edit_text(_mode_text(new_mode), reply_markup=_mode_keyboard(new_mode))
    except Exception:
        pass


def _mode_text(mode: str) -> str:
    mi = "●" if mode == "manual" else "○"
    ai = "●" if mode == "auto"   else "○"
    return (
        "╭━━━〔 🌌 TEMPEST MODE 〕━━━╮\n"
        f"┃  {mi}  Manual      ·  type filename per file\n"
        f"┃  {ai}  Auto Rename ·  template-based automatic\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
        "<i>⚡ Great Sage awaits your selection.</i>"
    )


def _mode_keyboard(current: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(("🟢 " if current == "manual" else "⚪ ") + "Manual",
                             callback_data="set_mode_manual"),
        InlineKeyboardButton(("🟢 " if current == "auto" else "⚪ ") + "Auto Rename",
                             callback_data="set_mode_auto"),
    ]])


# ══════════════════════════════════════════════════════════════════════════════
# /autorename
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command("autorename"))
async def cmd_autorename(client: Client, message: Message):
    user_id = message.from_user.id
    if await jishubotz.is_banned(user_id):
        return await message.reply("⛔ <b>Access Denied</b>")
    if not await jishubotz.is_premium(user_id):
        return await message.reply_text(
            "💎 <b>Premium Required</b>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("👤 My Status", callback_data="check_premium_status"),
            ]])
        )
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        current = await jishubotz.get_format_template(user_id)
        if current:
            return await message.reply_text(
                f"◈ <b>Auto Rename Template</b>\n\n"
                f"<b>Current:</b> <code>{current}</code>\n\n"
                f"➜ <code>/autorename &lt;template&gt;</code> to change.\n"
                f"Placeholders: <code>{{episode}}</code> <code>{{season}}</code> "
                f"<code>{{quality}}</code> <code>{{audio}}</code>"
            )
        return await message.reply_text(
            "◈ <b>Auto Rename Template</b>\n\nNo template saved yet.\n\n"
            "➜ <b>Usage:</b> <code>/autorename My Show S{season}E{episode} [{quality}]</code>"
        )
    template = parts[1].strip()
    await jishubotz.set_format_template(user_id, template)
    await message.reply_text(
        f"╭━━━〔 🧬 EVOLUTION TEMPLATE 〕━━━╮\n"
        f"┃  <code>{template}</code>\n"
        f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
        "✨ Template acquired. Enable /mode → Auto Rename, then send files."
    )


# ══════════════════════════════════════════════════════════════════════════════
# /setsource  –  what text to extract metadata FROM
# ══════════════════════════════════════════════════════════════════════════════

_SOURCE_LABELS = {
    "filename": "📄 File Name",
    "caption":  "📝 Caption Only",
    "both":     "🔀 Both  (caption → filename)",
}


@Client.on_message(filters.private & filters.command("setsource"))
async def cmd_setsource(client: Client, message: Message):
    user_id = message.from_user.id
    if await jishubotz.is_banned(user_id):
        return await message.reply("⛔ <b>Access Denied</b>")
    if not await jishubotz.is_premium(user_id):
        return await message.reply_text("💎 <b>Premium Required</b>")
    current = await jishubotz.get_rename_source(user_id)
    await message.reply_text(
        f"◈ <b>Metadata Source</b>\n\n"
        f"Where should I look for episode/season/quality info?\n\n"
        f"Current: <b>{_SOURCE_LABELS.get(current, current)}</b>",
        reply_markup=_source_keyboard(current),
    )


@Client.on_callback_query(filters.regex(r"^setsource_(filename|caption|both)$"))
async def cb_setsource(client: Client, update: CallbackQuery):
    user_id = update.from_user.id
    if not await jishubotz.is_premium(user_id):
        return await update.answer("💎 Premium required.", show_alert=True)
    src = update.data.split("_", 1)[1]
    await jishubotz.set_rename_source(user_id, src)
    await update.answer(f"✅ Source: {_SOURCE_LABELS[src]}", show_alert=True)
    try:
        await update.message.edit_text(
            f"◈ <b>Metadata Source</b>\n\nCurrent: <b>{_SOURCE_LABELS[src]}</b>",
            reply_markup=_source_keyboard(src),
        )
    except Exception:
        pass


def _source_keyboard(current: str) -> InlineKeyboardMarkup:
    rows = []
    for key, label in _SOURCE_LABELS.items():
        rows.append([InlineKeyboardButton(
            ("✅ " if current == key else "") + label,
            callback_data=f"setsource_{key}",
        )])
    return InlineKeyboardMarkup(rows)


# ══════════════════════════════════════════════════════════════════════════════
# /setmedia
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command("setmedia"))
async def cmd_setmedia(client: Client, message: Message):
    user_id = message.from_user.id
    if await jishubotz.is_banned(user_id):
        return await message.reply("⛔ <b>Access Denied</b>")
    if not await jishubotz.is_premium(user_id):
        return await message.reply_text("💎 <b>Premium Required</b>")
    current = await jishubotz.get_media_preference(user_id)
    await message.reply_text(
        f"◈ <b>Auto Rename — Output Type</b>\n\n"
        f"Current: <b>{current or 'auto-detect'}</b>\n\n"
        "Choose how renamed files should be sent:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📄 Document", callback_data="setmedia_document")],
            [InlineKeyboardButton("🎥 Video",    callback_data="setmedia_video")],
            [InlineKeyboardButton("🎵 Audio",    callback_data="setmedia_audio")],
        ]),
    )


@Client.on_callback_query(filters.regex(r"^setmedia_(document|video|audio)$"))
async def cb_setmedia(client: Client, update: CallbackQuery):
    user_id    = update.from_user.id
    media_type = update.data.split("_", 1)[1]
    await jishubotz.set_media_preference(user_id, media_type)
    await update.answer(f"Output type set to {media_type} ✅")
    await update.message.edit_text(
        f"◈ <b>Output type:</b> <code>{media_type}</code> ✅"
    )


# ══════════════════════════════════════════════════════════════════════════════
# /autoqueue  –  live queue panel
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command("autoqueue"))
async def cmd_autoqueue(client: Client, message: Message):
    user_id = message.from_user.id
    if await jishubotz.is_banned(user_id):
        return await message.reply("⛔ <b>Access Denied</b>")
    text, markup = _queue_panel(user_id)
    await message.reply_text(text, reply_markup=markup)


@Client.on_callback_query(filters.regex(r"^aq_refresh$"))
async def cb_aq_refresh(client: Client, update: CallbackQuery):
    text, markup = _queue_panel(update.from_user.id)
    try:
        await update.message.edit_text(text, reply_markup=markup)
    except Exception:
        pass
    await update.answer("Refreshed ✅")


@Client.on_callback_query(filters.regex(r"^aq_cancel_(.+)$"))
async def cb_aq_cancel(client: Client, update: CallbackQuery):
    user_id = update.from_user.id
    job_id  = update.data[len("aq_cancel_"):]
    if await _cancel_job(job_id, user_id):
        await update.answer(f"✅ Job {job_id} cancelled.", show_alert=True)
    else:
        await update.answer("❌ Job not found or already finished.", show_alert=True)
    text, markup = _queue_panel(user_id)
    try:
        await update.message.edit_text(text, reply_markup=markup)
    except Exception:
        pass


def _queue_panel(user_id: int):
    my_ids = _user_jobs.get(user_id, set())
    if not my_ids:
        return (
            "╭━━━〔 📋 TASK QUEUE 〕━━━╮\n┃  ✨  No active jobs.\n╰━━━━━━━━━━━━━━━━━━━━━━━━╯",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data="aq_refresh")]]),
        )

    active_ids  = set(_active_jobs.keys())
    waiting_ids = {j.job_id for j in _waiting_queue}
    waiting_list = list(_waiting_queue)

    lines   = ["╭━━━〔 📋 TASK QUEUE 〕━━━╮"]
    buttons = []

    for jid in sorted(my_ids):
        if jid in active_ids:
            job  = _active_jobs[jid]
            icon = {"downloading": "↓", "processing": "⚙", "uploading": "↑"}.get(job.status, "·")
            lines.append(
                f"<code>{jid}</code>  {icon}  {job.status.upper()}\n"
                f"   <i>{_trim(job.short_name(), 40)}</i>"
            )
            buttons.append([InlineKeyboardButton(f"✕ {jid}", callback_data=f"aq_cancel_{jid}")])
        elif jid in waiting_ids:
            job = next((j for j in waiting_list if j.job_id == jid), None)
            pos  = waiting_list.index(job) + 1 if job else "?"
            name = job.short_name() if job else jid
            lines.append(
                f"<code>{jid}</code>  ·  queued  ·  pos {pos}\n"
                f"   <i>{_trim(name, 40)}</i>"
            )
            buttons.append([InlineKeyboardButton(f"✕ {jid}", callback_data=f"aq_cancel_{jid}")])

    # Clean finished jobs
    finished = my_ids - active_ids - waiting_ids
    for jid in finished:
        my_ids.discard(jid)
    if not my_ids:
        _user_jobs.pop(user_id, None)

    lines.append("╰━━━━━━━━━━━━━━━━━━━━━━━━╯")
    buttons.append([InlineKeyboardButton("🔄 Refresh", callback_data="aq_refresh")])
    return "\n".join(lines), InlineKeyboardMarkup(buttons)


@Client.on_message(filters.private & filters.command("clearqueue"))
async def cmd_clearqueue(client: Client, message: Message):
    """Admin-only: wipe the entire pending_jobs collection and reset in-memory state."""
    from config import Config as _Cfg
    if message.from_user.id not in _Cfg.ADMIN:
        return await message.reply_text("⛔ <b>Admin only.</b>")
    async with _queue_lock:
        # Cancel all active tasks
        for job in list(_active_jobs.values()):
            if job.task and not job.task.done():
                job.task.cancel()
        _active_jobs.clear()
        _waiting_queue.clear()
        _user_jobs.clear()
    await jishubotz.clear_all_pending_jobs()
    await message.reply_text(
        "╭━━━〔 🗑 QUEUE CLEARED 〕━━━╮\n"
        "┃  All pending jobs wiped.\n"
        "┃  DB collection cleared.\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
    )


async def _cancel_job(job_id: str, user_id: int) -> bool:
    async with _queue_lock:
        job = _active_jobs.get(job_id)
        if job and job.user_id == user_id:
            if job.task and not job.task.done():
                job.task.cancel()
            job.status = "cancelled"
            return True
        for j in list(_waiting_queue):
            if j.job_id == job_id and j.user_id == user_id:
                _waiting_queue.remove(j)
                j.status = "cancelled"
                _user_jobs.get(user_id, set()).discard(job_id)
                # Remove from persistent store — no need to restore on restart
                asyncio.create_task(jishubotz.delete_pending_job(job_id))
                try:
                    await j.message.reply_text(
                        f"╭━━━〔 🗑 QUEUE 〕━━━╮\n"
                        f"┃  Job <code>{job_id}</code> cancelled.\n"
                        f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
                    )
                except Exception:
                    pass
                return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# Background scheduler
# ══════════════════════════════════════════════════════════════════════════════

async def _scheduler(client: Client) -> None:
    while True:
        await asyncio.sleep(1)
        async with _queue_lock:
            while len(_active_jobs) < CONCURRENT_JOBS and _waiting_queue:
                job = _waiting_queue.popleft()
                if job.status == "cancelled":
                    continue
                job.status = "downloading"
                _active_jobs[job.job_id] = job
                job.task = asyncio.create_task(
                    _run_pipeline(client, job),
                    name=f"auto_rename_{job.job_id}",
                )


def start_scheduler(client: Client) -> None:
    """Call once from bot startup after the client is running."""
    asyncio.create_task(_scheduler(client), name="auto_rename_scheduler")
    asyncio.create_task(_restore_pending_jobs(client), name="auto_rename_restore")


async def _restore_pending_jobs(client: Client) -> None:
    """
    On startup: load every job that was saved to MongoDB before the restart,
    re-fetch the original Telegram message and re-enqueue it.

    Jobs whose messages are no longer accessible (deleted / expired) are
    silently dropped and removed from the DB.
    """
    # Small delay so the scheduler loop is already running before we flood it
    await asyncio.sleep(2)

    try:
        pending = await jishubotz.load_all_pending_jobs()
    except Exception as e:
        logger.error("[restore] Failed to load pending jobs from DB: %s", e)
        return

    if not pending:
        return

    logger.info("[restore] Found %d pending job(s) to restore after restart.", len(pending))

    restored = 0
    for doc in pending:
        job_id     = doc["_id"]
        user_id    = int(doc["user_id"])
        chat_id    = int(doc["chat_id"])
        message_id = int(doc["message_id"])
        queued_at  = float(doc.get("queued_at", time.time()))
        file_name  = doc.get("file_name", "unknown")

        try:
            message = await client.get_messages(chat_id, message_id)
            if not message or not message.media:
                raise ValueError("message gone or has no media")
        except Exception as e:
            logger.warning(
                "[restore] job=%s message %s/%s unreachable (%s) — dropping.",
                job_id, chat_id, message_id, e,
            )
            await jishubotz.delete_pending_job(job_id)
            continue

        # Notify user their job is being re-queued
        try:
            await client.send_message(
                chat_id,
                f"♻️ <b>Bot restarted</b> — your file is back in queue.\n"
                f"┃  🆔  <code>{job_id}</code>\n"
                f"┃  📂  <code>{file_name}</code>",
            )
        except Exception:
            pass

        job = _Job(
            job_id=job_id,
            user_id=user_id,
            message=message,
            queued_at=queued_at,
        )

        async with _queue_lock:
            _user_jobs.setdefault(user_id, set()).add(job_id)
            if len(_active_jobs) < CONCURRENT_JOBS:
                job.status = "downloading"
                _active_jobs[job_id] = job
                job.task = asyncio.create_task(
                    _run_pipeline(client, job),
                    name=f"auto_rename_{job_id}",
                )
            else:
                _waiting_queue.append(job)
                job.status = "queued"

        restored += 1
        logger.info("[restore] Re-enqueued job=%s for user=%s", job_id, user_id)

    logger.info("[restore] Restored %d / %d pending job(s).", restored, len(pending))


# ══════════════════════════════════════════════════════════════════════════════
# Public entry point  (called by file_rename.rename_start when mode == "auto")
# ══════════════════════════════════════════════════════════════════════════════

async def run_auto_rename(client: Client, message: Message) -> None:
    user_id = message.from_user.id

    fmt = await jishubotz.get_format_template(user_id)
    if not fmt:
        return await message.reply_text(
            "╭━━━〔 ⚠️ GREAT SAGE WARNING 〕━━━╮\n"
            "┃  No Auto Rename template set.\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
            "➤  /autorename My Show S{season}E{episode} [{quality}]\n"
            "➤  /mode  ·  switch to Manual"
        )

    # ── Size gate (before consuming a queue slot) ─────────────────────────
    from config import Config as _Cfg
    from helper.userbot import userbot_available
    try:
        _file_obj  = getattr(message, message.media.value, None) if message.media else None
        _file_size = getattr(_file_obj, "file_size", 0) or 0
    except Exception:
        _file_size = 0
    if _file_size > _Cfg.BOT_MAX_SIZE:
        is_prem = await jishubotz.is_premium(user_id)
        if not is_prem:
            return await message.reply_text(
                "╭━━━〔 ⚠️ BARRIER LIMIT 〕━━━╮\n"
                "┃  📦  Exceeds 2 GB bot limit.\n"
                "╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
                "👑  Upgrade to Tempest Elite for 4 GB support."
            )
        if not userbot_available():
            return await message.reply_text(
                "╭━━━〔 ⚠️ BARRIER LIMIT 〕━━━╮\n"
                "┃  📦  Exceeds 2 GB — userbot not set.\n"
                "╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
                "Contact the admin to enable large-file support."
            )
        if _file_size > _Cfg.USER_MAX_SIZE:
            return await message.reply_text(
                "╭━━━〔 ⚠️ BARRIER LIMIT 〕━━━╮\n"
                "┃  📦  Exceeds 4 GB — maximum barrier.\n"
                "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
            )

    allowed, used, limit = await _check_daily(user_id)
    if not allowed:
        # Silently queue — user will be notified when limit clears tomorrow.
        # We still enqueue and let the pipeline handle it gracefully so the
        # user's files are not dropped; the job will fail at pipeline start
        # if the counter has not reset, but in practice the queue drains
        # within the same day so this path is mainly a soft safety valve.
        # We just accept the file into queue; no noisy rejection message.
        pass  # fall through to enqueue below

    job_id = await _new_job_id()
    job    = _Job(job_id=job_id, user_id=user_id, message=message)

    # ── Determine original filename for DB record ─────────────────────────
    try:
        _f = (getattr(message, message.media.value, None)
              if message.media else None)
        _orig_fname = getattr(_f, "file_name", None) or str(message.media.value)
    except Exception:
        _orig_fname = "unknown"

    # ── Persist to MongoDB BEFORE touching in-memory state ────────────────
    # This guarantees the job survives a crash/restart even if it never
    # makes it out of the waiting queue.
    await jishubotz.save_pending_job(
        job_id     = job_id,
        user_id    = user_id,
        chat_id    = message.chat.id,
        message_id = message.id,
        file_name  = _orig_fname,
        queued_at  = time.time(),
    )

    async with _queue_lock:
        _user_jobs.setdefault(user_id, set()).add(job_id)
        if len(_active_jobs) < CONCURRENT_JOBS:
            job.status = "downloading"
            _active_jobs[job_id] = job
            job.task = asyncio.create_task(
                _run_pipeline(client, job),
                name=f"auto_rename_{job_id}",
            )
            pos_text = "▶️ Starting immediately"
        else:
            _waiting_queue.append(job)
            pos      = len(_waiting_queue)
            pos_text = f"⏳ Position in queue: <b>{pos}</b>"

    # For queued jobs send a lightweight waiting card; starting jobs get the
    # progress message from inside _run_pipeline instead.
    if pos_text.startswith("⏳"):
        pos_num = pos_text.split("<b>")[1].split("</b>")[0]
        job.accept_msg = await message.reply_text(
            f"◈ <b>Auto Rename</b>  <code>[{job_id}]</code>\n\n"
            f"╭━━━〔 💠 RIMURU SYSTEM 〕━━━╮\n"
            f"┃  ⏳  Queued  ·  position {pos_num}\n"
            f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🗑 Cancel Job", callback_data=f"aq_cancel_{job_id}"),
                InlineKeyboardButton("📋 View Queue", callback_data="aq_refresh"),
            ]]),
        )


# ══════════════════════════════════════════════════════════════════════════════
# Pipeline
# ══════════════════════════════════════════════════════════════════════════════

async def _run_pipeline(client: Client, job: _Job) -> None:
    message = job.message
    user_id = job.user_id
    job_id  = job.job_id

    download_path: Optional[str] = None
    metadata_path: Optional[str] = None
    ph_path:       Optional[str] = None
    status_msg                   = None

    try:
        # ── Identify file ─────────────────────────────────────────────────
        if message.document:
            file_obj, file_name, base_media = message.document, message.document.file_name or "file", "document"
        elif message.video:
            file_obj, file_name, base_media = message.video, message.video.file_name or "video", "video"
        elif message.audio:
            file_obj, file_name, base_media = message.audio, message.audio.file_name or "audio", "audio"
        else:
            return await message.reply_text("❌ Unsupported file type.")

        file_caption = (message.caption or "").strip()

        # ── Choose client: userbot for >2 GB files ────────────────────────
        from config import Config as _Cfg
        from helper.userbot import get_userbot, userbot_available
        _file_size = getattr(file_obj, "file_size", 0) or 0
        _large     = _file_size > _Cfg.BOT_MAX_SIZE
        _dl_client = client
        _ul_client = client

        if _large:
            if not await jishubotz.is_premium(user_id):
                return await message.reply_text(
                    "╭━━━〔 ⚠️ BARRIER LIMIT 〕━━━╮\n"
                    "┃  📦  Exceeds 2 GB bot limit.\n"
                    "╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
                    "👑  Upgrade to Tempest Elite for 4 GB support."
                )
            if not userbot_available():
                return await message.reply_text(
                    "╭━━━〔 ⚠️ BARRIER LIMIT 〕━━━╮\n"
                    "┃  📦  Exceeds 2 GB — STRING_SESSION not set.\n"
                    "╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
                    "Contact the admin to enable large-file support."
                )
            if _file_size > _Cfg.USER_MAX_SIZE:
                return await message.reply_text(
                    "╭━━━〔 ⚠️ BARRIER LIMIT 〕━━━╮\n"
                    "┃  📦  Exceeds 4 GB — maximum barrier.\n"
                    "╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
                )
            _ub = await get_userbot()
            if _ub:
                _dl_client = _ub
                _ul_client = _ub
                logger.info("[auto_rename] job=%s using userbot for %s MB file",
                            job_id, _file_size // 1024 // 1024)
            else:
                return await message.reply_text(
                    "╭━━━〔 ⚠️ BARRIER LIMIT 〕━━━╮\n"
                    "┃  📦  Exceeds 2 GB — userbot failed to start.\n"
                    "╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
                    "Contact the admin to check STRING_SESSION."
                )

        # ── ONE round-trip: all settings + premium + daily count ─────────────
        _ps             = await jishubotz.get_pipeline_settings(user_id)
        source          = _ps["rename_source"]
        fmt             = _ps["format_template"]
        thumb_id        = _ps["thumbnail"]
        raw_cap         = _ps["caption"]
        prefix          = _ps["prefix"]
        suffix          = _ps["suffix"]
        media_pref      = _ps["auto_media_type"]
        use_metadata    = _ps["metadata"]
        metadata_fields = _ps["metadata_fields"]

        # ── Daily limit (uses values already in _ps — zero extra round-trips) ─
        is_prem_pipe = _ps["premium"]
        if not is_prem_pipe:
            used_now  = _ps["auto_daily_count"]
            day_limit = await jishubotz.get_auto_daily_limit()
            if used_now >= day_limit:
                try:
                    if status_msg:
                        await status_msg.edit_text(
                            f"╭━━━〔 ⚡ DAILY LIMIT 〕━━━╮\n"
                            f"┃  📊  {used_now}/{day_limit} used today\n"
                            f"┃  ⏱   Resets at midnight UTC\n"
                            f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
                            f"Upgrade to <b>Tempest Elite</b> for unlimited access 👑"
                        )
                except Exception:
                    pass
                return
            await _inc_daily(user_id)

        # ── Determine extraction source ───────────────────────────────────
        if source == "caption":
            extraction_text = file_caption if file_caption else file_name
        elif source == "both":
            extraction_text = (file_caption + " " + file_name).strip() if file_caption else file_name
        else:   # "filename" (default)
            extraction_text = file_name

        # ── Build new filename ────────────────────────────────────────────
        new_file_name    = _apply_template(fmt, extraction_text, file_name)
        new_file_name_ps = add_prefix_suffix(new_file_name, prefix, suffix)

        # ── Status message (replaces queued card if any) ─────────────────
        cancel_kb  = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"🗑 Cancel  [{job_id}]", callback_data=f"aq_cancel_{job_id}")
        ]])
        # Delete the "queued" card now that we're actually running
        if job.accept_msg:
            try:
                await job.accept_msg.delete()
            except Exception:
                pass
            job.accept_msg = None

        status_msg     = await message.reply_text(
            _status_text(job_id, new_file_name_ps, "downloading", 0, 0),
            reply_markup=cancel_kb,
        )
        job.status_msg = status_msg

        # ── Paths ─────────────────────────────────────────────────────────
        folder        = str(user_id)
        download_path = os.path.join("downloads", folder, new_file_name_ps)
        metadata_path = os.path.join("Metadata",  folder, f"{job_id}_{new_file_name_ps}")
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

        # ── Download ──────────────────────────────────────────────────────
        job.status  = "downloading"
        last_edit   = [0.0]
        t0          = time.time()

        async def _prog(cur, total, smsg, start):
            if time.time() - last_edit[0] < _PROGRESS_THROTTLE:
                return
            last_edit[0] = time.time()
            pct = cur * 100 // total if total else 0
            spd = cur / max(time.time() - start, 1)
            try:
                await smsg.edit_text(
                    _status_text(job_id, new_file_name_ps, job.status, pct, spd),
                    reply_markup=cancel_kb,
                )
            except Exception:
                pass

        from plugins.file_rename import get_transmission_sem as _get_tsem
        async with _get_tsem():
            file_path = await _dl_client.download_media(
                message, file_name=download_path,
                progress=_prog, progress_args=(status_msg, t0),
            )

        # ── Metadata (universal — same toggle/fields as manual rename) ────
        job.status      = "processing"
        _meta_applied   = False
        try:
            await status_msg.edit_text(
                _status_text(job_id, new_file_name_ps, "processing", 100, 0),
                reply_markup=cancel_kb,
            )
        except Exception:
            pass

        _has_metadata_values = any((v or "").strip() for v in metadata_fields.values())
        if use_metadata and _has_metadata_values:
            result = await add_metadata(file_path, metadata_path, metadata_fields, status_msg)
            if result and os.path.exists(metadata_path):
                file_path     = metadata_path
                _meta_applied = True
            else:
                logger.warning("[auto_rename] metadata returned None for job=%s", job_id)

        # ── Duration ──────────────────────────────────────────────────────
        duration = 0
        try:
            duration = await get_duration_hachoir(file_path)
        except Exception:
            pass

        # ── Thumbnail (universal — same DB field as manual rename) ────────
        if thumb_id:
            try:
                ph_path = await client.download_media(thumb_id)
            except Exception:
                pass
        elif base_media == "video" and message.video and message.video.thumbs:
            try:
                ph_path = await client.download_media(message.video.thumbs[0].file_id)
            except Exception:
                pass

        # ── Actual file size from disk (after metadata processing) ───────
        actual_size = os.path.getsize(file_path) if os.path.exists(file_path) else (getattr(file_obj, "file_size", 0) or 0)

        # ── Caption (universal — same template as manual rename) ──────────
        if raw_cap:
            try:
                caption = raw_cap.format(
                    filename=new_file_name_ps,
                    filesize=humanbytes(actual_size),
                    duration=convert(duration) if duration else "N/A",
                )
            except Exception:
                caption = f"<b>{new_file_name_ps}</b>"
        else:
            caption = f"<b>{new_file_name_ps}</b>"

        # ── Upload ────────────────────────────────────────────────────────
        job.status   = "uploading"
        last_edit[0] = 0.0
        try:
            await status_msg.edit_text(
                _status_text(job_id, new_file_name_ps, "uploading", 0, 0),
                reply_markup=cancel_kb,
            )
        except Exception:
            pass

        upload_type = media_pref or base_media
        t0          = time.time()

        common = dict(
            chat_id=message.chat.id, caption=caption, thumb=ph_path,
            progress=_prog, progress_args=(status_msg, t0),
        )
        async with _get_tsem():
            if upload_type == "document":
                sent = await _ul_client.send_document(document=file_path, file_name=new_file_name_ps, **common)
            elif upload_type == "video":
                if duration: common["duration"] = int(duration)
                sent = await _ul_client.send_video(video=file_path, **common)
            else:
                if duration: common["duration"] = int(duration)
                sent = await _ul_client.send_audio(audio=file_path, **common)

        # ── Leaderboard + history ─────────────────────────────────────────
        try:
            from plugins.leaderboard import record_rename, record_history
            display  = message.from_user.first_name or str(user_id)
            asyncio.create_task(record_rename(user_id, display))
            asyncio.create_task(record_history(user_id, new_file_name_ps, actual_size))
        except Exception:
            pass

        # ── Log to BIN/LOG channel (caption + thumb) ─────────────────────
        try:
            from config import Config as _Cfg
            _log_cap = (
                f"📂 <b>{file_caption}</b>\n➜ ✏️ <b>{new_file_name_ps}</b>"
                if file_caption else
                f"📂 <b>{file_name}</b>\n➜ ✏️ <b>{new_file_name_ps}</b>"
            )
            await client.copy_message(
                chat_id=_Cfg.BIN_CHANNEL,
                from_chat_id=message.chat.id,
                message_id=sent.id,
                caption=_log_cap,
            )
        except Exception as _le:
            logger.warning("[auto_rename] BIN_CHANNEL log failed job=%s: %s", job_id, _le)

        # ── Dump channel (universal — same setting as manual rename) ──────
        try:
            if _ps.get("dump_mode") and _ps.get("dump_channel"):
                from plugins.file_rename import _dump_to_channel
                asyncio.create_task(
                    _dump_to_channel(client, user_id, int(_ps["dump_channel"]), sent)
                )
        except Exception:
            pass

        job.status = "done"
        # Clean up: delete progress message and the original file message
        for _m in (status_msg, message):
            try:
                await _m.delete()
            except Exception:
                pass

    except asyncio.CancelledError:
        job.status = "cancelled"
        try:
            if status_msg:
                await status_msg.edit_text(
                    f"╭━━━〔 🗑 CANCELLED 〕━━━╮\n"
                    f"┃  🆔  <code>{job_id}</code>\n"
                    f"┃  ⚡  Task removed.\n"
                    f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
                )
        except Exception:
            pass
        try:
            await message.delete()
        except Exception:
            pass

    except Exception as e:
        job.status = "error"
        logger.exception("[auto_rename] pipeline error job=%s user=%s", job_id, user_id)
        try:
            if status_msg:
                await status_msg.edit_text(
                    f"╭━━━〔 ❌ SKILL FAILED 〕━━━╮\n"
                    f"┃  🆔  <code>{job_id}</code>\n"
                    f"┃  ⚠️  <code>{e}</code>\n"
                    f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
                )
        except Exception:
            pass

    finally:
        async with _queue_lock:
            _active_jobs.pop(job_id, None)
            _user_jobs.get(user_id, set()).discard(job_id)
        # Job is done (success / error / cancel) — remove from persistent store
        try:
            await jishubotz.delete_pending_job(job_id)
        except Exception as _dpe:
            logger.warning("[auto_rename] delete_pending_job failed job=%s: %s", job_id, _dpe)
        for path in (download_path, metadata_path):
            if path and os.path.exists(path):
                try: os.remove(path)
                except Exception: pass
        if ph_path and os.path.exists(ph_path):
            try: os.remove(ph_path)
            except Exception: pass


def _status_text(job_id: str, name: str, phase: str, pct: int, speed: float) -> str:
    phase_map = {
        "downloading": "⬇️  Acquiring File",
        "processing":  "🧬  Evolving Data",
        "uploading":   "⬆️  Transmitting",
        "done":        "✨  Evolution Complete",
    }
    label    = phase_map.get(phase, f"⚡  {phase.upper()}")
    bar      = "█" * (pct // 10) + "░" * (10 - pct // 10)
    spd_str  = f"  ·  {humanbytes(speed)}/s" if speed > 0 else ""
    name_str = _trim(name, 44)
    lines = [
        f"╭━━━〔 💠 RIMURU SYSTEM 〕━━━╮",
        f"┃  🆔  <code>{job_id}</code>",
        f"┃  📂  <code>{name_str}</code>",
        f"┣━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"┃  {label}",
    ]
    if pct > 0 or phase in ("downloading", "uploading"):
        lines.append(f"┃  <code>[{bar}]</code>  {pct}%{spd_str}")
    lines.append(f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# Template engine
# ══════════════════════════════════════════════════════════════════════════════

def _apply_template(fmt: str, extraction_text: str, file_name: str) -> str:
    """Fill fmt placeholders using data extracted from extraction_text; keep file_name extension."""
    ep  = _extract_episode_number(extraction_text)
    s   = _extract_season_number(extraction_text)
    aud = _extract_audio_info(extraction_text)
    q   = _extract_quality(extraction_text)

    sfmt = str(s)            if s  is not None else "1"   # season: no zero-pad  (S1, S2 …)
    efmt = str(ep).zfill(2) if ep is not None else "01"  # 01,02...09,10,11...100+

    t = fmt
    t = re.sub(r'S(?:Season|season|SEASON)(\d+)', f'S{sfmt}', t, flags=re.IGNORECASE)
    for pat in [re.compile(r'\{season\}', re.IGNORECASE),
                re.compile(r'\bseason\b',  re.IGNORECASE),
                re.compile(r'Season[\s._-]*\d*', re.IGNORECASE)]:
        t = pat.sub(sfmt, t)

    t = re.sub(r'EP(?:Episode|episode|EPISODE)', f'EP{efmt}', t, flags=re.IGNORECASE)
    for pat in [re.compile(r'\{episode\}', re.IGNORECASE),
                re.compile(r'\bEpisode\b',  re.IGNORECASE),
                re.compile(r'\bEP\b',       re.IGNORECASE)]:
        t = pat.sub(efmt, t)

    ar = aud or ""
    for pat in [re.compile(r'\{audio\}',   re.IGNORECASE),
                re.compile(r'\bAudio\b',   re.IGNORECASE)]:
        t = pat.sub(ar, t)

    qr = q or ""
    for pat in [re.compile(r'\{quality\}', re.IGNORECASE),
                re.compile(r'\bQuality\b', re.IGNORECASE)]:
        t = pat.sub(qr, t)

    t = re.sub(r'\[\s*\]', '', t)
    t = re.sub(r'\(\s*\)', '', t)
    t = re.sub(r'\{\s*\}', '', t)
    t = t.strip()

    _, ext = os.path.splitext(file_name)
    if ext and not t.lower().endswith(ext.lower()):
        t = f"{t}{ext}"
    return t


# ══════════════════════════════════════════════════════════════════════════════
# Extraction helpers
# ══════════════════════════════════════════════════════════════════════════════

_QUAL_INDS = [
    r'\d{2,4}[pP]', r'\dK', r'HD(?:RIP)?', r'WEB(?:-)?DL', r'BLURAY',
    r'X264', r'X265', r'HEVC', r'FHD', r'UHD', r'HDR', r'H\.264', r'H\.265',
    r'(?:19|20)\d{2}', r'Multi(?:audio)?', r'Dual(?:audio)?',
]
_QPAT = r'(?:' + '|'.join(r'(?:[\s._-]*' + q + r')' for q in _QUAL_INDS) + r')'
_SKIP = {360, 480, 720, 1080, 1440, 2160, 2020, 2021, 2022, 2023, 2024, 2025}


def _extract_episode_number(text: str):
    if not text: return None
    patterns = [
        re.compile(r'S\d+[.-_]?E(\d+)',                                            re.IGNORECASE),
        re.compile(r'(?:Episode|EP)[\s._-]*(\d+)',                                 re.IGNORECASE),
        re.compile(r'\bE(\d+)\b',                                                  re.IGNORECASE),
        re.compile(r'[\[\(]E(\d+)[\]\)]',                                          re.IGNORECASE),
        re.compile(r'\b(\d+)\s*of\s*\d+\b',                                        re.IGNORECASE),
        re.compile(r'(?:^|[^0-9A-Z])(\d{1,4})(?:[^0-9A-Z]|$)(?!' + _QPAT + r')', re.IGNORECASE),
    ]
    for pat in patterns:
        for m in pat.findall(text):
            raw = m[0] if isinstance(m, tuple) else m
            try:
                n = int(raw)
                if 1 <= n <= 9999 and n not in _SKIP:
                    return n
            except ValueError:
                pass
    return None


def _extract_season_number(text: str):
    if not text: return None
    patterns = [
        re.compile(r'S(\d+)[._-]?E\d+',                      re.IGNORECASE),
        re.compile(r'(?:Season|SEASON|season)[\s._-]*(\d+)', re.IGNORECASE),
        re.compile(r'\bS(\d+)\b(?!E\d|' + _QPAT + r')',     re.IGNORECASE),
        re.compile(r'[\[\(]S(\d+)[\]\)]',                    re.IGNORECASE),
        re.compile(r'[._-]S(\d+)(?:[._-]|$)',                re.IGNORECASE),
    ]
    for pat in patterns:
        m = pat.search(text)
        if m:
            try:
                n = int(m.group(1))
                if 1 <= n <= 99: return n
            except ValueError:
                pass
    return None


def _extract_audio_info(text: str):
    kw = {
        'Hindi': r'Hindi', 'English': r'English', 'Multi': r'Multi(?:audio)?',
        'Telugu': r'Telugu', 'Tamil': r'Tamil', 'Jap': r'Jap',
        'Dual': r'Dual(?:audio)?', 'AAC': r'AAC', 'AC3': r'AC3',
        'DTS': r'DTS', '5.1': r'5\.1',
    }
    found = [k for k, p in kw.items() if re.search(p, text, re.IGNORECASE)]
    return ' '.join(found) if found else None


def _extract_quality(text: str):
    for pat in [
        re.compile(r'\b(4K|2K|2160p|1440p|1080p|720p|480p|360p)\b', re.IGNORECASE),
        re.compile(r'\b(HD(?:RIP)?|WEB(?:-)?DL|BLURAY)\b',           re.IGNORECASE),
        re.compile(r'\b(X264|X265|HEVC)\b',                           re.IGNORECASE),
    ]:
        m = pat.search(text)
        if m: return m.group(1)
    return None
