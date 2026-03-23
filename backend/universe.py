# File: backend/universe.py
from __future__ import annotations

import os

from .utils_symbols import normalize_symbol

DEFAULT_UNIVERSE = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT", "BNB/USDT", "TRX/USDT"]


def get_universe_symbols() -> list[str]:
  
    raw = (os.getenv("V1_FORCE_UNIVERSE_SYMBOLS") or "").strip()
    if not raw:
        return DEFAULT_UNIVERSE[:]

    parts = [p.strip() for p in raw.split(",") if p.strip()]
    out = [normalize_symbol(p) for p in parts]
    return out or DEFAULT_UNIVERSE[:]