# File: backend/risk_limits.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping

from .utils_symbols import normalize_symbol


@dataclass(frozen=True)
class EquitySnapshot:
    equity_usdt: float
    cash_usdt: float
    assets_usdt: float


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _extract_balances(raw: Any) -> dict[str, float]:
    """
    Accepts:
      - dict like {"USDT": 200, "BTC": 0.01}
      - list like [{"asset": "USDT", "balance": 200}, ...]
      - list like [("USDT", 200), ...]
    Returns canonical dict asset->amount
    """
    if raw is None:
        return {}

    if isinstance(raw, Mapping):
        out: dict[str, float] = {}
        for k, v in raw.items():
            asset = str(k).upper().strip()
            out[asset] = _to_float(v, 0.0)
        return out

    if isinstance(raw, list):
        out = {}
        for item in raw:
            if isinstance(item, Mapping):
                asset = str(item.get("asset") or item.get("symbol") or item.get("currency") or "").upper().strip()
                amt = _to_float(item.get("balance") if "balance" in item else item.get("amount"), 0.0)
                if asset:
                    out[asset] = out.get(asset, 0.0) + amt
            elif isinstance(item, (tuple, list)) and len(item) >= 2:
                asset = str(item[0]).upper().strip()
                amt = _to_float(item[1], 0.0)
                if asset:
                    out[asset] = out.get(asset, 0.0) + amt
        return out

    return {}


def compute_paper_equity_snapshot(db, price_fn) -> EquitySnapshot:
    """
    price_fn(symbol_ccxt: str) -> float|None
    Uses paper_balances as source of truth if available.
    """
    # 1) Read balances
    raw_bal = None
    if hasattr(db, "list_paper_balances"):
        raw_bal = db.list_paper_balances()
    balances = _extract_balances(raw_bal)

    cash_usdt = _to_float(balances.get("USDT"), 0.0)
    assets_usdt = 0.0

    for asset, amt in balances.items():
        if asset == "USDT":
            continue
        if amt <= 0:
            continue
        sym = normalize_symbol(f"{asset}/USDT")
        px = price_fn(sym)
        if px is None:
            continue
        assets_usdt += float(px) * float(amt)

    equity = max(0.0, cash_usdt + assets_usdt)
    return EquitySnapshot(equity_usdt=equity, cash_usdt=cash_usdt, assets_usdt=assets_usdt)


def compute_open_positions_unrealized(db, price_fn) -> float:
    """
    Unrealized PnL from open positions table (if you keep positions separate from balances).
    This is NOT used to compute equity if balances already reflect asset holdings,
    but it is useful for realized+unrealized loss check when your DB schema tracks positions.
    """
    unrealized = 0.0
    try:
        positions = db.get_open_positions()
    except Exception:
        return 0.0

    for p in positions or []:
        sym = normalize_symbol(str(p.get("symbol") or ""))
        entry = _to_float(p.get("entry_price"), 0.0)
        amt = _to_float(p.get("amount"), 0.0)
        if not sym or entry <= 0 or amt <= 0:
            continue
        px = price_fn(sym)
        if px is None:
            continue
        unrealized += (float(px) - float(entry)) * float(amt)

    return float(unrealized)


def resolve_daily_loss_limit_usdt(equity_usdt: float) -> float:
    """
    Priority:
      - DAILY_MAX_LOSS_PCT (default 0.02) gives dynamic limit = equity * pct
      - DAILY_MAX_LOSS_USDT (optional) gives fixed cap
    We enforce the STRICTER one: min(fixed, dynamic), ignoring zeros.
    """
    pct = _to_float(os.getenv("DAILY_MAX_LOSS_PCT", "0.02"), 0.02)
    pct = max(0.0, min(0.20, pct))  # clamp to sane bounds (0..20%)

    dynamic = float(equity_usdt) * float(pct) if equity_usdt > 0 and pct > 0 else 0.0

    fixed = _to_float(os.getenv("DAILY_MAX_LOSS_USDT", "0"), 0.0)
    fixed = abs(fixed)

    candidates = [x for x in (dynamic, fixed) if x and x > 0]
    if not candidates:
        return 0.0
    return float(min(candidates))