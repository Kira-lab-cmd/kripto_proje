from __future__ import annotations

import os
from typing import Any, Dict

import pandas as pd

from .indicators import Indicators
from .utils_symbols import normalize_symbol

GATE_STATUS_KEYS = (
    "ema_uptrend",
    "bias_not_down",
    "atr_ok",
    "squeeze_ok",
    "volume_ok",
    "breakout_ok",
)

ALL_GATE_STATUS_KEYS = (
    "ema_uptrend",
    "bias_1h_up",
    "bias_not_down",
    "atr_ok",
    "squeeze_ok",
    "volume_ok",
    "breakout_ok",
)


class TradingStrategy:
    """
    V1 SIMPLE BREAKOUT STRATEGY

    Amaç:
    - 1H trend yukarıysa
    - 15m breakout geliyorsa
    - hacim destekliyorsa
    - volatilite trade edilebilir seviyedeyse
    BUY üretmek.

    Not:
    - Bu strateji SHORT / aktif SELL sinyali üretmez.
    - Exit tarafı mevcut SL/TP + watchdog/trailing ile yönetilir.
    """

    def __init__(self) -> None:
        self.breakout_lookback = int(os.getenv("BREAKOUT_LOOKBACK", "20"))
        self.squeeze_window = int(os.getenv("SQUEEZE_WINDOW", "12"))

        self.min_atr_pct = float(os.getenv("MIN_ATR_PCT", "0.003"))   # %0.4
        self.max_atr_pct = float(os.getenv("MAX_ATR_PCT", "0.035"))   # %3.5

        self.min_volume_ratio = float(os.getenv("MIN_VOLUME_RATIO", "0.75"))
        self.breakout_buffer_pct = float(os.getenv("BREAKOUT_BUFFER_PCT", "0.0"))  # %0.05
        self.max_squeeze_range_pct = float(os.getenv("MAX_SQUEEZE_RANGE_PCT", "0.05"))  # %2.5

        self.atr_sl_mult = float(os.getenv("ATR_SL_MULT", "1.5"))
        self.atr_tp_mult = float(os.getenv("ATR_TP_MULT", "2.5"))

    def get_config_snapshot(self) -> Dict[str, Any]:
        return {
            "breakout_lookback": int(self.breakout_lookback),
            "squeeze_window": int(self.squeeze_window),
            "min_atr_pct": float(self.min_atr_pct),
            "max_atr_pct": float(self.max_atr_pct),
            "min_volume_ratio": float(self.min_volume_ratio),
            "breakout_buffer_pct": float(self.breakout_buffer_pct),
            "max_squeeze_range_pct": float(self.max_squeeze_range_pct),
            "atr_sl_mult": float(self.atr_sl_mult),
            "atr_tp_mult": float(self.atr_tp_mult),
        }

    @staticmethod
    def _ema(values: list[float], period: int) -> float | None:
        if not values or len(values) < period:
            return None
        try:
            s = pd.Series(values, dtype="float64")
            return float(s.ewm(span=period, adjust=False).mean().iloc[-1])
        except Exception:
            return None

    @staticmethod
    def _mean(values: list[float]) -> float | None:
        if not values:
            return None
        try:
            return float(sum(values) / len(values))
        except Exception:
            return None

    def _hold(
        self,
        *,
        reason: str,
        current_price: float | None,
        is_uptrend: bool,
        ema200: float | None,
        atr: float | None,
        atr_pct: float | None,
        vol_ratio: float | None,
        trend_dir_1h: str | None,
        gate_status: dict[str, bool] | None = None,
        hold_fail_reasons: list[str] | None = None,
        score: float = 0.0,
        regime: str = "NO_SETUP",
    ) -> Dict[str, Any]:
        gate_status = dict(gate_status or {name: False for name in ALL_GATE_STATUS_KEYS})
        for name in ALL_GATE_STATUS_KEYS:
            gate_status.setdefault(name, False)
        hold_fail_reasons = list(hold_fail_reasons or [name for name in GATE_STATUS_KEYS if not bool(gate_status.get(name, False))])
        return {
            "signal": "HOLD",
            "score": float(score),
            "current_price": current_price,
            "stop_loss": None,
            "take_profit": None,
            "reason": reason,
            "is_uptrend": bool(is_uptrend),
            "ema200": round(float(ema200), 8) if ema200 is not None else None,
            "atr": float(atr) if atr is not None else None,
            "atr_pct": float(atr_pct) if atr_pct is not None else None,
            "vol_ratio": float(vol_ratio) if vol_ratio is not None else None,
            "regime": regime,
            "regime_conf": 1.0 if regime == "BREAKOUT_READY" else 0.0,
            "adx": None,
            "er": None,
            "dir_1h": (trend_dir_1h or "UNKNOWN").upper(),
            "base_thresholds": {"buy": 5.0, "sell": -999.0},
            "threshold_bias": {"model": "simple_breakout_v1"},
            "effective_thresholds": {"buy": 5.0, "sell": -999.0},
            "gate_status": gate_status,
            "hold_fail_reasons": hold_fail_reasons,
        }

    def get_signal(
        self,
        ohlcv_data: list,
        sentiment_score: float,  # bilinçli olarak kullanılmıyor
        *,
        symbol: str | None = None,
        profile: dict[str, Any] | None = None,  # v1'de kullanılmıyor
        trend_dir_1h: str | None = None,
    ) -> Dict[str, Any]:
        if not ohlcv_data or len(ohlcv_data) < 200:
            return self._hold(
                reason="Yetersiz veri",
                current_price=None,
                is_uptrend=False,
                ema200=None,
                atr=None,
                atr_pct=None,
                vol_ratio=None,
                trend_dir_1h=trend_dir_1h,
                gate_status={name: False for name in ALL_GATE_STATUS_KEYS},
                hold_fail_reasons=list(GATE_STATUS_KEYS),
                regime="NO_DATA",
            )

        closes = [float(c[4]) for c in ohlcv_data]
        highs = [float(c[2]) for c in ohlcv_data]
        lows = [float(c[3]) for c in ohlcv_data]
        volumes = [float(c[5]) for c in ohlcv_data]

        current_price = float(closes[-1])
        previous_close = float(closes[-2])
        previous_high = float(highs[-2])

        ema200 = self._ema(closes, 200)
        is_uptrend = bool(ema200 is not None and current_price > ema200)

        atr = Indicators.calculate_atr(highs, lows, closes)
        atr_pct = (float(atr) / current_price) if atr and current_price > 0 else None

        lookback = max(5, self.breakout_lookback)
        squeeze_window = max(5, self.squeeze_window)

        breakout_level = max(closes[-lookback - 1:-1])
        breakout_buffer = breakout_level * (1.0 + self.breakout_buffer_pct)

        squeeze_high = max(highs[-squeeze_window - 1:-1])
        squeeze_low = min(lows[-squeeze_window - 1:-1])
        squeeze_range_pct = (squeeze_high - squeeze_low) / current_price if current_price > 0 else None

        avg_volume = self._mean(volumes[-lookback - 1:-1])
        vol_ratio = (volumes[-1] / avg_volume) if avg_volume and avg_volume > 0 else None

        dir_1h = (trend_dir_1h or "UNKNOWN").upper()
        sym = normalize_symbol(symbol or "")

        reasons: list[str] = []
        score = 0.0

        if is_uptrend:
            score += 1.0
            reasons.append("EMA200 trend=UP")
        else:
            reasons.append("EMA200 trend!=UP")

        bias_1h_up = bool(dir_1h == "UP")
        bias_not_down = bool(dir_1h in {"UP", "NEUTRAL"})
        if bias_1h_up:
            score += 1.0
            reasons.append("1H bias=UP")
        elif dir_1h == "NEUTRAL":
            reasons.append("1H bias=NEUTRAL (allowed)")
        elif dir_1h == "DOWN":
            reasons.append("1H bias=DOWN")
        else:
            reasons.append(f"1H bias={dir_1h}")

        atr_ok = atr_pct is not None and self.min_atr_pct <= atr_pct <= self.max_atr_pct
        if atr_ok:
            score += 1.0
            reasons.append(f"ATR% ok ({atr_pct * 100:.2f})")
        else:
            reasons.append("ATR gate fail")

        squeeze_ok = squeeze_range_pct is not None and squeeze_range_pct <= self.max_squeeze_range_pct
        if squeeze_ok:
            score += 1.0
            reasons.append(f"squeeze ok ({squeeze_range_pct * 100:.2f})")
        else:
            reasons.append("squeeze yok / range geniş")

        volume_ok = vol_ratio is not None and vol_ratio >= self.min_volume_ratio
        if volume_ok:
            score += 1.0
            reasons.append(f"VOLx ok ({vol_ratio:.2f})")
        else:
            reasons.append("volume confirm yok")

        # Breakout confirmation:
        # - previous candle must break the breakout level with its HIGH (wick breakout)
        # - current candle must CLOSE above the breakout level (confirmation)
        breakout_ok = current_price > breakout_buffer
        if breakout_ok:
            score += 2.0
            reasons.append("close breakout")
        else:
            reasons.append("breakout yok")

        gate_status = {
            "ema_uptrend": bool(is_uptrend),
            "bias_1h_up": bool(bias_1h_up),
            "bias_not_down": bool(bias_not_down),
            "atr_ok": bool(atr_ok),
            "squeeze_ok": bool(squeeze_ok),
            "volume_ok": bool(volume_ok),
            "breakout_ok": bool(breakout_ok),
        }
        hold_fail_reasons = [name for name in GATE_STATUS_KEYS if not bool(gate_status.get(name, False))]

        should_buy = all([
            bias_not_down,
            atr_ok,
            volume_ok,
            breakout_ok,
        ])

        if not should_buy:
            regime = "BREAKOUT_READY" if (is_uptrend and bias_not_down and atr_ok and squeeze_ok) else "NO_SETUP"
            if sym:
                reasons.append(f"SYM={sym}")
            return self._hold(
                reason=", ".join(reasons),
                current_price=current_price,
                is_uptrend=is_uptrend,
                ema200=ema200,
                atr=atr,
                atr_pct=atr_pct,
                vol_ratio=vol_ratio,
                trend_dir_1h=dir_1h,
                gate_status=gate_status,
                hold_fail_reasons=hold_fail_reasons,
                score=score,
                regime=regime,
            )

        stop_loss = current_price - (self.atr_sl_mult * float(atr))
        take_profit = current_price + (self.atr_tp_mult * float(atr))

        if sym:
            reasons.append(f"SYM={sym}")

        return {
            "signal": "BUY",
            "score": float(score),
            "current_price": float(current_price),
            "stop_loss": round(float(stop_loss), 8),
            "take_profit": round(float(take_profit), 8),
            "reason": ", ".join(reasons),
            "is_uptrend": bool(is_uptrend),
            "ema200": round(float(ema200), 8) if ema200 is not None else None,
            "atr": float(atr) if atr is not None else None,
            "atr_pct": float(atr_pct) if atr_pct is not None else None,
            "vol_ratio": float(vol_ratio) if vol_ratio is not None else None,
            "regime": "BREAKOUT_READY",
            "regime_conf": 1.0,
            "adx": None,
            "er": None,
            "dir_1h": dir_1h,
            "base_thresholds": {"buy": 5.0, "sell": -999.0},
            "threshold_bias": {"model": "simple_breakout_v1"},
            "effective_thresholds": {"buy": 5.0, "sell": -999.0},
            "gate_status": gate_status,
            "hold_fail_reasons": [],
        }
