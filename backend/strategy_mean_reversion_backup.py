"""
Mean Reversion Strategy V1

Buy the DIP, Sell the BOUNCE!

Entry Logic:
- RSI < 30 (oversold)
- Price < BB_lower (below 2 std dev)
- Volume confirmation
- EMA trend filter (optional)

Exit Logic:
- RSI > 70 (overbought) OR
- Price > BB_middle (mean revert) OR
- Stop loss hit

Expected Win Rate: 50-60%
"""

from __future__ import annotations

import os
from typing import Any, Dict

import pandas as pd

from .indicators import Indicators
from .utils_symbols import normalize_symbol

GATE_STATUS_KEYS = (
    "ema_uptrend",
    "rsi_oversold",
    "bb_oversold",
    "volume_ok",
)

ALL_GATE_STATUS_KEYS = (
    "ema_uptrend",
    "rsi_oversold",
    "bb_oversold",
    "volume_ok",
    "bias_not_down",
)


class TradingStrategy:
    """
    Mean Reversion Strategy V1
    
    Buy oversold conditions, sell when price reverts to mean.
    """

    def __init__(self) -> None:
        # RSI settings
        self.rsi_period = int(os.getenv("RSI_PERIOD", "14"))
        self.rsi_oversold = float(os.getenv("RSI_OVERSOLD", "30"))
        self.rsi_overbought = float(os.getenv("RSI_OVERBOUGHT", "70"))
        
        # Bollinger Bands settings
        self.bb_period = int(os.getenv("BB_PERIOD", "20"))
        self.bb_std_dev = float(os.getenv("BB_STD_DEV", "2.0"))
        
        # Volume settings
        self.min_volume_ratio = float(os.getenv("MIN_VOLUME_RATIO", "0.75"))
        self.volume_lookback = int(os.getenv("VOLUME_LOOKBACK", "20"))
        
        # Risk management
        self.atr_sl_mult = float(os.getenv("ATR_SL_MULT", "2.0"))
        self.target_bb_middle = os.getenv("TARGET_BB_MIDDLE", "true").lower() == "true"
        self.atr_tp_mult = float(os.getenv("ATR_TP_MULT", "1.5"))
        
        # Filters
        self.use_ema_filter = os.getenv("USE_EMA_FILTER", "false").lower() == "true"
        self.min_gates_required = int(os.getenv("MIN_GATES_REQUIRED", "2"))

    def get_config_snapshot(self) -> Dict[str, Any]:
        return {
            "version": "mean_reversion_v1",
            "rsi_period": int(self.rsi_period),
            "rsi_oversold": float(self.rsi_oversold),
            "rsi_overbought": float(self.rsi_overbought),
            "bb_period": int(self.bb_period),
            "bb_std_dev": float(self.bb_std_dev),
            "min_volume_ratio": float(self.min_volume_ratio),
            "volume_lookback": int(self.volume_lookback),
            "atr_sl_mult": float(self.atr_sl_mult),
            "target_bb_middle": self.target_bb_middle,
            "atr_tp_mult": float(self.atr_tp_mult),
            "use_ema_filter": self.use_ema_filter,
            "min_gates_required": int(self.min_gates_required),
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

    def _calculate_bollinger_bands(
        self,
        closes: list[float],
        period: int,
        std_dev: float
    ) -> tuple[float | None, float | None, float | None]:
        """Calculate Bollinger Bands: (lower, middle, upper)"""
        if not closes or len(closes) < period:
            return None, None, None
        
        try:
            recent = closes[-period:]
            middle = self._mean(recent)
            std = self._std(recent)
            
            if middle is None or std is None:
                return None, None, None
            
            lower = middle - (std_dev * std)
            upper = middle + (std_dev * std)
            
            return lower, middle, upper
        except Exception:
            return None, None, None

    def _hold(
        self,
        *,
        reason: str,
        current_price: float | None,
        is_uptrend: bool,
        ema200: float | None,
        atr: float | None,
        vol_ratio: float | None,
        rsi: float | None,
        bb_lower: float | None,
        bb_middle: float | None,
        bb_upper: float | None,
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
            "vol_ratio": float(vol_ratio) if vol_ratio is not None else None,
            "rsi": float(rsi) if rsi is not None else None,
            "bb_lower": float(bb_lower) if bb_lower is not None else None,
            "bb_middle": float(bb_middle) if bb_middle is not None else None,
            "bb_upper": float(bb_upper) if bb_upper is not None else None,
            "regime": regime,
            "regime_conf": 0.5,
            "dir_1h": trend_dir_1h,
            "base_thresholds": {"buy": 2.0, "sell": -999.0},
            "threshold_bias": {"model": "mean_reversion_v1"},
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
        # Check data sufficiency
        min_required = max(self.bb_period, self.rsi_period, 200) + 10
        if not ohlcv_data or len(ohlcv_data) < min_required:
            return self._hold(
                reason="Insufficient data",
                current_price=None,
                is_uptrend=False,
                ema200=None,
                atr=None,
                vol_ratio=None,
                rsi=None,
                bb_lower=None,
                bb_middle=None,
                bb_upper=None,
                trend_dir_1h=trend_dir_1h,
                gate_status={name: False for name in ALL_GATE_STATUS_KEYS},
                hold_fail_reasons=list(GATE_STATUS_KEYS),
                regime="NO_DATA",
            )

        # Extract OHLCV
        closes = [float(c[4]) for c in ohlcv_data]
        highs = [float(c[2]) for c in ohlcv_data]
        lows = [float(c[3]) for c in ohlcv_data]
        volumes = [float(c[5]) for c in ohlcv_data]

        current_price = float(closes[-1])

        # Calculate indicators
        rsi = Indicators.calculate_rsi(closes, period=self.rsi_period)
        bb_lower, bb_middle, bb_upper = self._calculate_bollinger_bands(
            closes, self.bb_period, self.bb_std_dev
        )
        
        atr = Indicators.calculate_atr(highs, lows, closes)
        
        # EMA trend (optional filter)
        ema200 = self._ema(closes, 200)
        is_uptrend = bool(ema200 is not None and current_price > ema200)
        
        # Volume confirmation
        avg_volume = self._mean(volumes[-self.volume_lookback - 1:-1])
        vol_ratio = (volumes[-1] / avg_volume) if avg_volume and avg_volume > 0 else None
        
        dir_1h = (trend_dir_1h or "UNKNOWN").upper()
        sym = normalize_symbol(symbol or "")

        # Initialize
        reasons: list[str] = []
        score = 0.0

        # Gate 1: RSI Oversold
        rsi_oversold = rsi is not None and rsi < self.rsi_oversold
        if rsi_oversold:
            score += 2.0
            reasons.append(f"RSI oversold ({rsi:.1f} < {self.rsi_oversold})")
        else:
            rsi_val = rsi if rsi is not None else 0.0
            reasons.append(f"RSI not oversold ({rsi_val:.1f})")

        # Gate 2: BB Oversold
        bb_oversold = (
            bb_lower is not None and 
            bb_middle is not None and 
            current_price < bb_lower
        )
        if bb_oversold:
            score += 2.0
            pct_below = ((bb_lower - current_price) / current_price) * 100
            reasons.append(f"Below BB lower ({pct_below:.2f}% below)")
        else:
            reasons.append("Not below BB lower")

        # Gate 3: Volume Confirmation
        volume_ok = vol_ratio is not None and vol_ratio >= self.min_volume_ratio
        if volume_ok:
            score += 1.0
            reasons.append(f"Volume ok ({vol_ratio:.2f}x)")
        else:
            vol_val = vol_ratio if vol_ratio is not None else 0.0
            reasons.append(f"Volume weak ({vol_val:.2f}x)")

        # Gate 4: EMA Filter (optional)
        if self.use_ema_filter:
            if is_uptrend:
                score += 1.0
                reasons.append("EMA trend UP")
            else:
                reasons.append("EMA trend DOWN")
        
        # 1H Bias (info only, not a gate)
        bias_not_down = bool(dir_1h in {"UP", "NEUTRAL"})
        if dir_1h == "UP":
            reasons.append("1H bias UP")
        elif dir_1h == "NEUTRAL":
            reasons.append("1H bias NEUTRAL")
        else:
            reasons.append("1H bias DOWN")

        # Gate status
        gate_status = {
            "ema_uptrend": bool(is_uptrend),
            "rsi_oversold": bool(rsi_oversold),
            "bb_oversold": bool(bb_oversold),
            "volume_ok": bool(volume_ok),
            "bias_not_down": bool(bias_not_down),
        }
        
        hold_fail_reasons = [name for name in GATE_STATUS_KEYS if not bool(gate_status.get(name, False))]

        # Entry decision
        required_gates = [rsi_oversold, bb_oversold]
        if self.use_ema_filter:
            required_gates.append(is_uptrend)
        
        gates_passed = sum(required_gates)
        should_buy = gates_passed >= self.min_gates_required

        if not should_buy:
            if sym:
                reasons.append(f"SYM={sym}")
            return self._hold(
                reason=", ".join(reasons),
                current_price=current_price,
                is_uptrend=is_uptrend,
                ema200=ema200,
                atr=atr,
                vol_ratio=vol_ratio,
                rsi=rsi,
                bb_lower=bb_lower,
                bb_middle=bb_middle,
                bb_upper=bb_upper,
                trend_dir_1h=dir_1h,
                gate_status=gate_status,
                hold_fail_reasons=hold_fail_reasons,
                score=score,
                regime="NOT_OVERSOLD",
            )

        # BUY Signal - Calculate SL/TP
        stop_loss = current_price - (self.atr_sl_mult * float(atr))
        
        # Take profit: BB_middle or ATR-based
        if self.target_bb_middle and bb_middle is not None:
            take_profit = float(bb_middle)
        else:
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
            "vol_ratio": float(vol_ratio) if vol_ratio is not None else None,
            "rsi": float(rsi) if rsi is not None else None,
            "bb_lower": float(bb_lower) if bb_lower is not None else None,
            "bb_middle": float(bb_middle) if bb_middle is not None else None,
            "bb_upper": float(bb_upper) if bb_upper is not None else None,
            "regime": "OVERSOLD_BOUNCE",
            "regime_conf": 1.0,
            "dir_1h": dir_1h,
            "base_thresholds": {"buy": 2.0, "sell": -999.0},
            "threshold_bias": {"model": "mean_reversion_v1"},
            "effective_thresholds": {"buy": float(self.min_gates_required), "sell": -999.0},
            "gate_status": gate_status,
            "hold_fail_reasons": [],
        }
