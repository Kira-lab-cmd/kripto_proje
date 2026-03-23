# File: backend/risk_guards.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
from typing import Callable, Optional

import logging
import os

logger = logging.getLogger(__name__)

TR_TZ = ZoneInfo("Europe/Istanbul")


def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name, "").strip()
        return float(v) if v else float(default)
    except Exception:
        return float(default)


def _today_tr() -> date:
    return datetime.now(tz=TR_TZ).date()


def resolve_daily_loss_limit_usdt(equity_usdt: float) -> float:
    """
    Daily loss limit resolver.
    Priority:
      1) DAILY_LOSS_LIMIT_USDT (absolute)
      2) DAILY_LOSS_LIMIT_PCT  (percentage of equity)
      3) default 0 => disabled

    Safety:
      - equity_usdt <= 0 => return 0 (disabled)
      - clamp to >= 0
    """
    abs_limit = _env_float("DAILY_LOSS_LIMIT_USDT", 0.0)
    if abs_limit > 0:
        return float(abs_limit)

    pct = _env_float("DAILY_LOSS_LIMIT_PCT", 0.0)
    if pct <= 0:
        return 0.0

    if equity_usdt <= 0:
        return 0.0

    # pct can be provided as 1.0 meaning 1% or 0.01 meaning 1%
    if pct > 1.0:
        pct = pct / 100.0

    limit = float(equity_usdt) * float(pct)
    return max(0.0, float(limit))


def compute_open_positions_unrealized(db, price_fn: Callable[[str], Optional[float]]) -> float:
    """
    Computes unrealized PnL for all open positions.

    Expected db.get_open_positions() returns list[dict] with at least:
      - symbol (str)
      - entry_price (float)
      - amount (float)
      - side optional ("LONG"/"SHORT") OR infer spot-long if missing
    """
    try:
        positions = db.get_open_positions() if db else []
    except Exception as e:
        logger.warning("compute_open_positions_unrealized: db.get_open_positions failed: %s", e)
        return 0.0

    total = 0.0

    for p in positions or []:
        try:
            sym = (p.get("symbol") or "").strip()
            if not sym:
                continue

            entry = float(p.get("entry_price") or 0.0)
            amt = float(p.get("amount") or 0.0)
            if entry <= 0 or amt == 0:
                continue

            px = price_fn(sym)
            if px is None:
                # Fail-safe choice:
                # If we cannot value, we skip unrealized contribution (conservative for loss-limit?).
                # Alternative is to block trading elsewhere when price is None.
                continue

            side = (p.get("side") or "LONG").upper()
            if side in ("SHORT", "SELL"):
                pnl = (entry - float(px)) * amt
            else:
                pnl = (float(px) - entry) * amt

            total += float(pnl)
        except Exception:
            continue

    return float(total)


def check_daily_loss_and_kill_switch(
    *,
    db,
    price_fn: Callable[[str], Optional[float]],
    equity_usdt: float,
) -> bool:
    """
    Returns True if kill-switch triggered (bot disabled).
    Uses: today_realized + unrealized <= -daily_loss_limit
    Idempotent: if already disabled, returns True.
    """
    if not db:
        return False

    limit_usdt = resolve_daily_loss_limit_usdt(equity_usdt=float(equity_usdt))
    if limit_usdt <= 0:
        return False

    # realized (today)
    try:
        realized = float(db.get_today_realized_pnl())
    except Exception as e:
        logger.warning("check_daily_loss: get_today_realized_pnl failed: %s", e)
        realized = 0.0

    # unrealized
    try:
        unrealized = float(compute_open_positions_unrealized(db=db, price_fn=price_fn))
    except Exception as e:
        logger.warning("check_daily_loss: compute_open_positions_unrealized failed: %s", e)
        unrealized = 0.0

    total = realized + unrealized

    if total <= -abs(limit_usdt):
        # idempotent disable
        try:
            # If you have db.get_bot_enabled(), prefer that; otherwise set blindly.
            if hasattr(db, "get_bot_enabled"):
                if db.get_bot_enabled() is False:
                    return True
            db.set_bot_enabled(False)
        except Exception as e:
            logger.error("check_daily_loss: set_bot_enabled(False) failed: %s", e)
            # Even if DB write failed, treat as not-triggered to avoid lying
            return False

        logger.warning(
            "KILL_SWITCH: daily loss limit hit. total=%.4f limit=%.4f (realized=%.4f unrealized=%.4f)",
            total,
            limit_usdt,
            realized,
            unrealized,
        )
        return True

    return False