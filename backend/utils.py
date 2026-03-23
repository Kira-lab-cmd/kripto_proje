# File: backend/utils.py
from __future__ import annotations

import logging
import os

from .utils_symbols import normalize_symbol

logger = logging.getLogger(__name__)

DEFAULT_UNIVERSE = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT", "BNB/USDT", "TRX/USDT"]


def get_dynamic_coins(max_coins: int = 6, min_volume: float = 25_000_000) -> list[str]:
    """
    Fixed universe selector.

    We intentionally ignore:
      - CoinGecko trending/gainers
      - External narratives
      - Any symbol outside the defined universe

    Env override:
      UNIVERSE_SYMBOLS="BTC/USDT,ETH/USDT,SOL/USDT,XRP/USDT,BNB/USDT,TRX/USDT"
    """
    raw = (os.getenv("UNIVERSE_SYMBOLS") or "").strip()
    if raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        symbols = [normalize_symbol(p) for p in parts]
    else:
        symbols = DEFAULT_UNIVERSE[:]

    # deterministic order, trim
    out = symbols[: max(1, int(max_coins))]
    logger.info("Dinamik Havuz (%s coin): %s", len(out), out)
    return out