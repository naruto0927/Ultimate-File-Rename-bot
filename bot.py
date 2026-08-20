"""
bot.py — Entry point.
Koyeb-friendly: starts a minimal aiohttp health-check server alongside the bot.
"""

import asyncio
import logging
import os
from datetime import datetime

import pyrogram.utils
from aiohttp import web
from pytz import timezone
from pyrogram import Client, __version__
from pyrogram.raw.all import layer

from config import Config
from helper.database import jishubotz
from messages import log, Msg
from route import web_server

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
for _noisy in ("pyrogram", "aiohttp", "motor"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

# ── Expand Pyrogram's ID range (required for large channels) ──────────────────
pyrogram.utils.MIN_CHAT_ID    = -999_999_999_999
pyrogram.utils.MIN_CHANNEL_ID = -1_009_999_999_999


class Bot(Client):

    def __init__(self):
        super().__init__(
            name="renamer",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=Config.BOT_TOKEN,
            # Koyeb free tier: 512 MB RAM, 1 vCPU.
            # workers=200 spawns 200 OS threads → OOM crash when 2+ files arrive.
            # 4 workers handle I/O-bound Pyrogram dispatch; actual rename work
            # runs in asyncio tasks (no threads), so this is plenty.
            workers=4,
            plugins={"root": "plugins"},
            sleep_threshold=30,
            # Keep transmissions low — each one holds a TCP connection open.
            max_concurrent_transmissions=4,
        )

    async def start(self):
        await super().start()
        me            = await self.get_me()
        self.mention  = me.mention
        self.username = me.username
        self.uptime   = Config.BOT_UPTIME

        # ── MongoDB indexes (idempotent — safe to run every boot) ───────────
        await jishubotz.ensure_indexes()

        # ── Restore persisted limits from DB (including transmission_limit) ────
        from plugins.file_rename import load_limits_from_db
        await load_limits_from_db()
        # load_limits_from_db also rebuilds _transmission_sem with the DB value,
        # so Pyrogram's own max_concurrent_transmissions no longer needs to match —
        # our semaphore is the real gate. But keep them in sync for clarity.
        try:
            from helper.database import jishubotz as _db
            _t = await _db.get_transmission_limit()
            self.max_concurrent_transmissions = _t
        except Exception:
            pass

        # ── Health-check web server (Koyeb / Render keep-alive) ───────────────
        port = int(os.environ.get("PORT", 8000))
        runner = web.AppRunner(await web_server())
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", port).start()
        log.info(f"Health-check server running on port {port}")

        # ── Auto-rename queue scheduler ───────────────────────────────────────
        from plugins.auto_rename import start_scheduler
        start_scheduler(self)

        # ── Userbot for >2 GB files (optional) ───────────────────────────────
        from helper.userbot import get_userbot, userbot_available
        if userbot_available():
            ub = await get_userbot()
            if ub:
                log.info("Userbot ready — large-file (>2 GB) support enabled.")
            else:
                log.warning("STRING_SESSION set but userbot failed to start.")
        else:
            log.info("STRING_SESSION not set — large-file support disabled.")

        log.info(Msg.BOT_STARTED, name=me.first_name)

        # ── Notify admins ─────────────────────────────────────────────────────
        for admin_id in Config.ADMIN:
            try:
                await self.send_message(
                    admin_id,
                    f"╭━━━〔 ✨ TEMPEST ONLINE 〕━━━╮\n"
                    f"┃  ⚡  {me.mention} is online.\n"
                    f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
                )
            except Exception as e:
                log.warning(Msg.BOT_ADMIN_NOTIFY_ERR, admin_id=admin_id, error=e)

        # ── Log to channel ────────────────────────────────────────────────────
        if Config.LOG_CHANNEL:
            try:
                ist = datetime.now(timezone("Asia/Kolkata"))
                await self.send_message(
                    Config.LOG_CHANNEL,
                    f"╭━━━〔 🌌 RIMURU SYSTEM BOOT 〕━━━╮\n"
                    f"┃  🤖  {me.mention}\n"
                    f"┃  📅  {ist.strftime('%d %B %Y')}\n"
                    f"┃  ⏰  {ist.strftime('%I:%M:%S %p')} IST\n"
                    f"┃  🔧  v{__version__} · Layer {layer}\n"
                    f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
                )
            except Exception as e:
                log.warning(Msg.BOT_LOG_CHANNEL_ERR, error=e)

    async def stop(self):
        from helper.userbot import stop_userbot
        await stop_userbot()
        await super().stop()
        log.info(Msg.BOT_STOPPED, mention=self.mention)


Bot().run()
