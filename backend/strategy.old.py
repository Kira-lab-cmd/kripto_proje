"""
Trading Strategy V2 + REGIME ADAPTATION

V2 improvements:
- Fixed breakout logic (percentile-based)
- Added noise buffer
- Relaxed gates (3/4)

NEW - Regime Adaptation:
- Don't trade in CHOP markets
- Conservative in HIGH_VOL markets
- Normal in TREND markets
"""

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
    V2 + REGIME ADAPTATION
    
    Combines V2 improvements with regime awareness for better win rate.
    """

    def __init__(self) -> None:
        # Breakout settings
        self.breakout_lookback = int(os.getenv("BREAKOUT_LOOKBACK", "20"))
        self.breakout_method = os.getenv("BREAKOUT_METHOD", "percentile")
        self.breakout_percentile = float(os.getenv("BREAKOUT_PERCENTILE", "0.90"))
        
        # Squeeze settings
        self.squeeze_window = int(os.getenv("SQUEEZE_WINDOW", "12"))
        self.max_squeeze_range_pct = float(os.getenv("MAX_SQUEEZE_RANGE_PCT", "0.05"))

        # ATR settings
        self.min_atr_pct = float(os.getenv("MIN_ATR_PCT", "0.005"))
        self.max_atr_pct = float(os.getenv("MAX_ATR_PCT", "0.025"))

        # Volume settings
        self.min_volume_ratio = float(os.getenv("MIN_VOLUME_RATIO", "0.75"))

        # Buffer settings
        self.breakout_buffer_pct = float(os.getenv("BREAKOUT_BUFFER_PCT", "0.005"))

        # Stop/Take settings
        self.atr_sl_mult = float(os.getenv("ATR_SL_MULT", "2.0"))
        self.atr_tp_mult = float(os.getenv("ATR_TP_MULT", "3.0"))

        # Gate requirement
        self.min_gates_required = int(os.getenv("MIN_GATES_REQUIRED", "3"))
        
        # REGIME ADAPTATION (NEW!)
        self.regime_enabled = os.getenv("REGIME_ADAPTATION", "true").lower() == "true"
        self.avoid_chop = os.getenv("AVOID_CHOP", "true").lower() == "true"
        self.high_vol_conservative = os.getenv("HIGH_VOL_CONSERVATIVE", "true").lower() == "true"

    def get_config_snapshot(self) -> Dict[str, Any]:
        return {
            "version": "v2_regime",
            "breakout_lookback": int(self.breakout_lookback),
            "breakout_method": str(self.breakout_method),
            "breakout_percentile": float(self.breakout_percentile),
            "squeeze_window": int(self.squeeze_window),
            "min_atr_pct": float(self.min_atr_pct),
            "max_atr_pct": float(self.max_atr_pct),
            "min_volume_ratio": float(self.min_volume_ratio),
            "breakout_buffer_pct": float(self.breakout_buffer_pct),
            "max_squeeze_range_pct": float(self.max_squeeze_range_pct),
            "atr_sl_mult": float(self.atr_sl_mult),
            "atr_tp_mult": float(self.atr_tp_mult),
            "min_gates_required": int(self.min_gates_required),
            "regime_enabled": self.regime_enabled,
            "avoid_chop": self.avoid_chop,
            "high_vol_conservative": self.high_vol_conservative,
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

    @staticmethod
    def _std(values: list[float]) -> float | None:
        if not values or len(values) < 2:
            return None
        try:
            mean = sum(values) / len(values)
            variance = sum((x - mean) ** 2 for x in values) / len(values)
            return float(variance ** 0.5)
        except Exception:
            return None

    def _find_resistance(self, highs: list[float], current_price: float) -> float | None:
        if len(highs) < 50:
            return None
        
        resistances = []
        for i in range(2, len(highs) - 2):
            if (highs[i] > highs[i-1] and highs[i] > highs[i-2] and
                highs[i] > highs[i+1] and highs[i] > highs[i+2]):
                resistances.append(highs[i])
        
        if not resistances:
            return None
        
        above_current = [r for r in resistances if r > current_price]
        
        if not above_current:
            return None
        
        return min(above_current)

    def _calculate_breakout_level(
        self,
        closes: list[float],
        highs: list[float],
        current_price: float,
        method: str = "percentile"
    ) -> float:
        lookback = max(5, self.breakout_lookback)
        
        if method == "percentile":
            try:
                recent = closes[-lookback-1:-1]
                sorted_closes = sorted(recent)
                idx = int(len(sorted_closes) * self.breakout_percentile)
                return sorted_closes[idx]
            except Exception:
                return max(closes[-lookback-1:-1])
        
        elif method == "resistance":
            resistance = self._find_resistance(highs, current_price)
            if resistance:
                return resistance
            return self._calculate_breakout_level(closes, highs, current_price, "percentile")
        
        elif method == "bb":
            try:
                recent = closes[-lookback-1:-1]
                mean = self._mean(recent)
                std = self._std(recent)
                if mean and std:
                    return mean + (2.0 * std)
            except Exception:
                pass
            return self._calculate_breakout_level(closes, highs, current_price, "percentile")
        
        else:
            return max(closes[-lookback-1:-1])

    def _detect_regime(self, is_uptrend: bool, atr_pct: float | None) -> str:
        """
        Simple regime detection.
        
        Returns: "TREND" | "CHOP" | "HIGH_VOL"
        """
        # HIGH_VOL check
        if atr_pct and atr_pct > 0.025:
            return "HIGH_VOL"
        
        # CHOP check (no clear trend)
        if not is_uptrend:
            return "CHOP"
        
        # Default to TREND if uptrend
        return "TREND"

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
        gate_status: dict[str, bool],
        hold_fail_reasons: list[str],
        regime: str = "NO_SETUP",
        score: float | None = None,
    ) -> Dict[str, Any]:
        return {
            "signal": "HOLD",
            "score": float(score) if score is not None else 0.0,
            "current_price": float(current_price) if current_price is not None else None,
            "stop_loss": None,
            "take_profit": None,
            "reason": reason,
            "is_uptrend": bool(is_uptrend),
            "ema200": round(float(ema200), 8) if ema200 is not None else None,
            "atr": float(atr) if atr is not None else None,
            "atr_pct": float(atr_pct) if atr_pct is not None else None,
            "vol_ratio": float(vol_ratio) if vol_ratio is not None else None,
            "regime": regime,
            "regime_conf": 0.5,
            "adx": None,
            "er": None,
            "dir_1h": trend_dir_1h,
            "base_thresholds": {"buy": 5.0, "sell": -999.0},
            "threshold_bias": {"model": "v2_regime"},
            "effective_thresholds": {"buy": float(self.min_gates_required), "sell": -999.0},
            "gate_status": gate_status,
            "hold_fail_reasons": hold_fail_reasons,
        }

    def get_signal(
        self,
        ohlcv_data: list,
        sentiment_score: float,
        *,
        symbol: str | None = None,
        profile: dict[str, Any] | None = None,
        trend_dir_1h: str | None = None,
    ) -> Dict[str, Any]:
        if not ohlcv_data or len(ohlcv_data) < 200:
            return self._hold(
                reason="Insufficient data",
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
        current_high = float(highs[-1])

        ema200 = self._ema(closes, 200)
        is_uptrend = bool(ema200 is not None and current_price > ema200)

        atr = Indicators.calculate_atr(highs, lows, closes)
        atr_pct = (float(atr) / current_price) if atr and current_price > 0 else None

        # REGIME DETECTION
        regime = self._detect_regime(is_uptrend, atr_pct)
        
        # REGIME FILTERING (if enabled)
        if self.regime_enabled:
            # 1. CHOP - Don't trade!
            if self.avoid_chop and regime == "CHOP":
                return self._hold(
                    reason="CHOP regime - avoiding trade",
                    current_price=current_price,
                    is_uptrend=is_uptrend,
                    ema200=ema200,
                    atr=atr,
                    atr_pct=atr_pct,
                    vol_ratio=None,
                    trend_dir_1h=trend_dir_1h,
                    gate_status={name: False for name in ALL_GATE_STATUS_KEYS},
                    hold_fail_reasons=["regime_chop"],
                    regime="CHOP",
                )

        breakout_level = self._calculate_breakout_level(
            closes, highs, current_price, method=self.breakout_method
        )
        breakout_buffer = breakout_level * (1.0 + self.breakout_buffer_pct)

        squeeze_window = max(5, self.squeeze_window)
        squeeze_high = max(highs[-squeeze_window - 1:-1])
        squeeze_low = min(lows[-squeeze_window - 1:-1])
        squeeze_range_pct = (squeeze_high - squeeze_low) / current_price if current_price > 0 else None

        lookback = max(5, self.breakout_lookback)
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

        breakout_ok = current_high > breakout_buffer
        if breakout_ok:
            score += 2.0
            reasons.append(f"breakout {self.breakout_method} ({breakout_level:.2f})")
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

        # GATE REQUIREMENT (regime-dependent)
        min_gates = self.min_gates_required
        
        # HIGH_VOL: Be more conservative (require all 4 gates)
        if self.regime_enabled and self.high_vol_conservative and regime == "HIGH_VOL":
            min_gates = 4
        
        gate_count = sum([bias_not_down, atr_ok, volume_ok, breakout_ok])
        should_buy = gate_count >= min_gates

        if not should_buy:
            regime_str = "BREAKOUT_READY" if (is_uptrend and bias_not_down and atr_ok and squeeze_ok) else "NO_SETUP"
            if sym:
                reasons.append(f"SYM={sym}")
            if self.regime_enabled:
                reasons.append(f"REGIME={regime}")
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
                regime=regime_str,
            )

        stop_loss = current_price - (self.atr_sl_mult * float(atr))
        take_profit = current_price + (self.atr_tp_mult * float(atr))

        if sym:
            reasons.append(f"SYM={sym}")
        if self.regime_enabled:
            reasons.append(f"REGIME={regime}")

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
            "regime": regime,
            "regime_conf": 1.0,
            "adx": None,
            "er": None,
            "dir_1h": dir_1h,
            "base_thresholds": {"buy": 5.0, "sell": -999.0},
            "threshold_bias": {"model": "v2_regime"},
            "effective_thresholds": {"buy": float(min_gates), "sell": -999.0},
            "gate_status": gate_status,
            "hold_fail_reasons": [],
        }