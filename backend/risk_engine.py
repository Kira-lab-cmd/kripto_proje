# File: backend/risk_engine.py
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SizeResult:
    qty: float
    notional_usdt: float
    risk_usdt: float
    per_unit_risk: float
    reason: str


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(v)))


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def compute_qty_from_stop(
    *,
    symbol: str,
    entry_price: float,
    stop_loss: float | None,
    equity_usdt: float,
    risk_pct: float,
    exchange: Any,
    max_notional_pct: float | None = None,
) -> SizeResult:
    """Volatility-normalized spot sizing.

    Core invariant: risk_usdt = equity * risk_pct (capped) and qty = risk_usdt / (entry - stop).

    Notes:
      - Assumes spot-long for BUY sizing.
      - If stop_loss is missing/invalid, falls back to notional sizing: qty = (equity*risk_pct)/entry.
      - Applies a notional concentration cap: max_notional_pct of equity (default env MAX_NOTIONAL_PCT=0.25).
      - Rounds using exchange.amount_to_precision when available.
    """

    px = _safe_float(entry_price)
    if px <= 0:
        return SizeResult(0.0, 0.0, 0.0, 0.0, "entry_price_invalid")

    eq = max(0.0, _safe_float(equity_usdt))
    if eq <= 0:
        return SizeResult(0.0, 0.0, 0.0, 0.0, "equity_invalid")

    rp = _clamp(_safe_float(risk_pct), 0.0, 0.25)  # hard safety clamp (<=25% is still insane but prevents NaNs)
    risk_usdt = eq * rp
    if risk_usdt <= 0:
        return SizeResult(0.0, 0.0, 0.0, 0.0, "risk_pct_zero")

    per_unit_risk = 0.0
    sl = _safe_float(stop_loss, 0.0) if stop_loss is not None else 0.0
    if 0 < sl < px:
        per_unit_risk = px - sl

    # fee+slippage floor: if stop is too tight, treat per_unit_risk as at least MIN_STOP_PCT*px
    try:
        min_stop_pct = float(os.getenv("MIN_STOP_PCT", "0.0015"))  # 0.15% default
        min_stop_pct = _clamp(min_stop_pct, 0.0, 0.05)
    except Exception:
        min_stop_pct = 0.0015

    if per_unit_risk > 0:
        per_unit_risk = max(per_unit_risk, px * min_stop_pct)
        qty = risk_usdt / per_unit_risk
        sizing_reason = "stop_risk_sizing"
    else:
        # Fallback: notional sizing
        qty = (risk_usdt / px)
        sizing_reason = "fallback_notional_sizing"

    # Concentration cap
    if max_notional_pct is None:
        try:
            max_notional_pct = float(os.getenv("MAX_NOTIONAL_PCT", "0.25"))
        except Exception:
            max_notional_pct = 0.25
    max_notional_pct = _clamp(float(max_notional_pct), 0.0, 1.0)
    max_notional = eq * max_notional_pct
    notional = qty * px
    if max_notional > 0 and notional > max_notional:
        qty = max_notional / px
        notional = qty * px
        sizing_reason = f"{sizing_reason}|capped_max_notional"

    # Round via exchange precision
    qty_n = qty
    try:
        if exchange is not None and hasattr(exchange, "amount_to_precision"):
            qty_n = float(exchange.amount_to_precision(symbol, float(qty)))
    except Exception:
        qty_n = float(qty)

    if qty_n <= 0:
        return SizeResult(0.0, 0.0, float(risk_usdt), float(per_unit_risk), "qty_zero_after_precision")

    return SizeResult(
        qty=float(qty_n),
        notional_usdt=float(qty_n) * px,
        risk_usdt=float(risk_usdt),
        per_unit_risk=float(per_unit_risk),
        reason=sizing_reason,
    )
