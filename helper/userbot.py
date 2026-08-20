"""
helper/userbot.py
──────────────────
Lazily initialised Pyrogram userbot client used to handle files > 2 GB.

Usage
-----
    from helper.userbot import get_userbot, userbot_available

    if userbot_available():
        ub = await get_userbot()
        await ub.download_media(...)

The client is started once and reused across calls. If STRING_SESSION is not
set the module stays dormant and userbot_available() returns False.
"""

from __future__ import annotations

import logging
from typing import Optional

from pyrogram import Client

from config import Config

logger = logging.getLogger(__name__)

_userbot: Optional[Client] = None
_started: bool = False


def userbot_available() -> bool:
    """True when a STRING_SESSION is configured."""
    return bool(getattr(Config, "STRING_SESSION", ""))


async def get_userbot() -> Optional[Client]:
    """
    Return the running userbot Client, starting it on first call.
    Returns None if STRING_SESSION is not set or startup failed.
    """
    global _userbot, _started

    if not getattr(Config, "STRING_SESSION", ""):
        return None

    if _started and _userbot is not None:
        return _userbot

    try:
        _userbot = Client(
            name="userbot",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            session_string=Config.STRING_SESSION,
            no_updates=True,      # we only use it for file transfer, not handling updates
            in_memory=True,
        )
        await _userbot.start()
        me = await _userbot.get_me()
        logger.info("Userbot started: %s (id=%s)", me.first_name, me.id)
        _started = True
        return _userbot
    except Exception as e:
        logger.error("Userbot failed to start: %s", e)
        _userbot = None
        _started = False
        return None


async def stop_userbot() -> None:
    """Gracefully stop the userbot (called from bot shutdown)."""
    global _userbot, _started
    if _userbot and _started:
        try:
            await _userbot.stop()
        except Exception:
            pass
        _userbot = None
        _started = False
