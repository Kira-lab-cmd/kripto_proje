from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .utils_symbols import normalize_symbol


@dataclass(frozen=True)
class CoinProfile:
    symbol: str
    # Strategy thresholds
    buy_threshold: float
    sell_threshold: float
    min_volume_ratio: float
    min_atr_pct: float
    max_atr_pct: float

    # Trend penalties/boosts (aggression knobs)
    downtrend_buy_penalty: float
    uptrend_buy_boost: float

    # Risk scaling (multiplies DB risk_multiplier)
    risk_mult: float

    @staticmethod
    def from_row(row: dict[str, Any]) -> "CoinProfile":
        return CoinProfile(
            symbol=normalize_symbol(str(row["symbol"])),
            buy_threshold=float(row["buy_threshold"]),
            sell_threshold=float(row["sell_threshold"]),
            min_volume_ratio=float(row["min_volume_ratio"]),
            min_atr_pct=float(row["min_atr_pct"]),
            max_atr_pct=float(row["max_atr_pct"]),
            downtrend_buy_penalty=float(row["downtrend_buy_penalty"]),
            uptrend_buy_boost=float(row["uptrend_buy_boost"]),
            risk_mult=float(row["risk_mult"]),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "buy_threshold": self.buy_threshold,
            "sell_threshold": self.sell_threshold,
            "min_volume_ratio": self.min_volume_ratio,
            "min_atr_pct": self.min_atr_pct,
            "max_atr_pct": self.max_atr_pct,
            "downtrend_buy_penalty": self.downtrend_buy_penalty,
            "uptrend_buy_boost": self.uptrend_buy_boost,
            "risk_mult": self.risk_mult,
        }


def default_profile(symbol: str) -> CoinProfile:
    """
    Conservative-by-default, but still trade-capable in 15m/1h.
    Aggression will be applied by strategy when AGGRESSIVE_MODE=1.
    """
    sym = normalize_symbol(symbol)
    base = CoinProfile(
        symbol=sym,
        buy_threshold=3.0,
        sell_threshold=-3.0,
        min_volume_ratio=0.50,
        min_atr_pct=0.002,   # 0.20%
        max_atr_pct=0.060,   # 6.0%
        downtrend_buy_penalty=2.0,
        uptrend_buy_boost=1.0,
        risk_mult=1.0,
    )

    # Slightly more tolerant on higher-vol alts
    if sym in {"SOL/USDT", "XRP/USDT", "TRX/USDT"}:
        return CoinProfile(**{**base.as_dict(), "buy_threshold": 2.0, "max_atr_pct": 0.080, "risk_mult": 0.80})
    if sym in {"BNB/USDT"}:
        return CoinProfile(**{**base.as_dict(), "buy_threshold": 2.5, "max_atr_pct": 0.070, "risk_mult": 0.90})
    return base


def derive_profile_from_research(symbol: str, vol_ann: float, max_drawdown: float) -> CoinProfile:
    """
    Heuristic derivation:
      - higher vol -> lower thresholds (more signals), but lower risk_mult
      - deeper drawdown -> reduce risk_mult
    """
    sym = normalize_symbol(symbol)

    # vol tiers
    if vol_ann >= 0.75:
        buy_th = 2.0
        max_atr = 0.090
        risk_mult = 0.70
    elif vol_ann >= 0.55:
        buy_th = 2.5
        max_atr = 0.080
        risk_mult = 0.80
    else:
        buy_th = 3.0
        max_atr = 0.070
        risk_mult = 0.90

    # drawdown penalty
    if max_drawdown <= -0.65:
        risk_mult *= 0.80
    elif max_drawdown <= -0.55:
        risk_mult *= 0.90

    risk_mult = max(0.30, min(1.00, risk_mult))

    return CoinProfile(
        symbol=sym,
        buy_threshold=float(buy_th),
        sell_threshold=-float(buy_th),
        min_volume_ratio=0.45,
        min_atr_pct=0.0018,
        max_atr_pct=float(max_atr),
        downtrend_buy_penalty=1.5,
        uptrend_buy_boost=1.0,
        risk_mult=float(risk_mult),
    )