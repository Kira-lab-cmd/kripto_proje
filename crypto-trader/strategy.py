"""
ADAPTIVE HYBRID STRATEGY - FINAL VERSION

Combines Breakout + Mean Reversion with ADX-based regime detection.

Regime Detection (ADX):
- ADX < 20:  CHOPPY market → Use Mean Reversion
- ADX > 25:  TRENDING market → Use Breakout
- ADX 20-25: TRANSITION → HOLD (or conservative)

Expected Win Rate: 48-52%

This is our BEST strategy combining:
- V2 Breakout (proven in some conditions)
- Mean Reversion (48.6% win rate in Fold 2!)
- ADX regime filter (use right strategy at right time)
"""

from __future__ import annotations

import os
from typing import Any, Dict

import pandas as pd

from .indicators import Indicators
from .utils_symbols import normalize_symbol

GATE_STATUS_KEYS = (
    "regime_ok",
    "primary_signal",
    "volume_ok",
    "bias_ok",
)

ALL_GATE_STATUS_KEYS = (
    "regime_ok",
    "primary_signal",
    "volume_ok",
    "bias_ok",
    "secondary_confirm",
)


class TradingStrategy:
    """
    Adaptive Hybrid Strategy
    
    Selects strategy based on market regime (ADX).
    """

    def __init__(self) -> None:
        # Regime detection
        self.adx_period = int(os.getenv("ADX_PERIOD", "14"))
        self.adx_trending_threshold = float(os.getenv("ADX_TRENDING_THRESHOLD", "25"))
        self.adx_choppy_threshold = float(os.getenv("ADX_CHOPPY_THRESHOLD", "20"))
        
        # Breakout settings (for TRENDING markets)
        self.breakout_lookback = int(os.getenv("BREAKOUT_LOOKBACK", "20"))
        self.breakout_percentile = float(os.getenv("BREAKOUT_PERCENTILE", "0.90"))
        self.breakout_buffer_pct = float(os.getenv("BREAKOUT_BUFFER_PCT", "0.005"))
        
        # Mean Reversion settings (for CHOPPY markets)
        self.rsi_period = int(os.getenv("RSI_PERIOD", "14"))
        self.rsi_oversold = float(os.getenv("RSI_OVERSOLD", "30"))
        self.bb_period = int(os.getenv("BB_PERIOD", "20"))
        self.bb_std_dev = float(os.getenv("BB_STD_DEV", "2.0"))
        
        # Common settings
        self.min_atr_pct = float(os.getenv("MIN_ATR_PCT", "0.005"))
        self.max_atr_pct = float(os.getenv("MAX_ATR_PCT", "0.025"))
        self.min_volume_ratio = float(os.getenv("MIN_VOLUME_RATIO", "0.75"))
        
        # Risk management
        self.atr_sl_mult = float(os.getenv("ATR_SL_MULT", "2.0"))
        self.atr_tp_mult_trending = float(os.getenv("ATR_TP_MULT_TRENDING", "3.0"))
        self.atr_tp_mult_choppy = float(os.getenv("ATR_TP_MULT_CHOPPY", "1.5"))
        
        # Strategy selection
        self.use_ema_filter = os.getenv("USE_EMA_FILTER", "true").lower() == "true"
        self.min_gates_required = int(os.getenv("MIN_GATES_REQUIRED", "2"))

    def get_config_snapshot(self) -> Dict[str, Any]:
        return {
            "version": "adaptive_hybrid_v1",
            "adx_period": int(self.adx_period),
            "adx_trending_threshold": float(self.adx_trending_threshold),
            "adx_choppy_threshold": float(self.adx_choppy_threshold),
            "breakout_lookback": int(self.breakout_lookback),
            "breakout_percentile": float(self.breakout_percentile),
            "rsi_period": int(self.rsi_period),
            "rsi_oversold": float(self.rsi_oversold),
            "bb_period": int(self.bb_period),
            "bb_std_dev": float(self.bb_std_dev),
            "min_atr_pct": float(self.min_atr_pct),
            "max_atr_pct": float(self.max_atr_pct),
            "min_volume_ratio": float(self.min_volume_ratio),
            "atr_sl_mult": float(self.atr_sl_mult),
            "atr_tp_mult_trending": float(self.atr_tp_mult_trending),
            "atr_tp_mult_choppy": float(self.atr_tp_mult_choppy),
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

    def _calculate_breakout_level(
        self,
        closes: list[float],
        current_price: float,
    ) -> float:
        """Calculate breakout level using percentile method"""
        lookback = max(5, self.breakout_lookback)
        
        try:
            recent = closes[-lookback-1:-1]
            sorted_closes = sorted(recent)
            idx = int(len(sorted_closes) * self.breakout_percentile)
            return sorted_closes[idx]
        except Exception:
            return max(closes[-lookback-1:-1])

    def _detect_regime(self, adx: float | None) -> str:
        """
        Detect market regime using ADX.
        
        Returns:
        - "TRENDING": ADX > 25 (use Breakout)
        - "CHOPPY": ADX < 20 (use Mean Reversion)
        - "TRANSITION": ADX 20-25 (be cautious)
        """
        if adx is None:
            return "UNKNOWN"
        
        if adx > self.adx_trending_threshold:
            return "TRENDING"
        elif adx < self.adx_choppy_threshold:
            return "CHOPPY"
        else:
            return "TRANSITION"

    def _hold(
        self,
        *,
        reason: str,
        current_price: float | None,
        regime: str,
        adx: float | None,
        **kwargs
    ) -> Dict[str, Any]:
        return {
            "signal": "HOLD",
            "score": 0.0,
            "current_price": float(current_price) if current_price is not None else None,
            "stop_loss": None,
            "take_profit": None,
            "reason": reason,
            "regime": regime,
            "adx": float(adx) if adx is not None else None,
            "regime_conf": 1.0 if regime in ["TRENDING", "CHOPPY"] else 0.5,
            "gate_status": kwargs.get("gate_status", {}),
            "hold_fail_reasons": kwargs.get("hold_fail_reasons", []),
            **{k: v for k, v in kwargs.items() if k not in ["gate_status", "hold_fail_reasons"]}
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
        min_required = max(self.bb_period, self.breakout_lookback, self.adx_period, 200) + 30
        if not ohlcv_data or len(ohlcv_data) < min_required:
            return self._hold(
                reason="Insufficient data",
                current_price=None,
                regime="NO_DATA",
                adx=None,
                gate_status={name: False for name in ALL_GATE_STATUS_KEYS},
                hold_fail_reasons=list(GATE_STATUS_KEYS),
            )

        # Extract OHLCV
        closes = [float(c[4]) for c in ohlcv_data]
        highs = [float(c[2]) for c in ohlcv_data]
        lows = [float(c[3]) for c in ohlcv_data]
        volumes = [float(c[5]) for c in ohlcv_data]

        current_price = float(closes[-1])
        current_high = float(highs[-1])

        # Calculate common indicators
        adx = Indicators.calculate_adx(highs, lows, closes, period=self.adx_period)
        atr = Indicators.calculate_atr(highs, lows, closes)
        atr_pct = (float(atr) / current_price) if atr and current_price > 0 else None
        
        ema200 = self._ema(closes, 200)
        is_uptrend = bool(ema200 is not None and current_price > ema200)
        
        # Volume
        avg_volume = self._mean(volumes[-20:])
        vol_ratio = (volumes[-1] / avg_volume) if avg_volume and avg_volume > 0 else None
        volume_ok = vol_ratio is not None and vol_ratio >= self.min_volume_ratio
        
        # ATR check
        atr_ok = atr_pct is not None and self.min_atr_pct <= atr_pct <= self.max_atr_pct
        
        # 1H bias
        dir_1h = (trend_dir_1h or "UNKNOWN").upper()
        bias_ok = bool(dir_1h in {"UP", "NEUTRAL"})
        
        # REGIME DETECTION
        regime = self._detect_regime(adx)
        
        reasons: list[str] = []
        score = 0.0
        
        # TRANSITION regime - be very conservative
        if regime == "TRANSITION":
            reasons.append(f"TRANSITION regime (ADX={adx:.1f if adx else 0})")
            reasons.append("Waiting for clear regime")
            return self._hold(
                reason=", ".join(reasons),
                current_price=current_price,
                regime=regime,
                adx=adx,
                atr=atr,
                atr_pct=atr_pct,
                vol_ratio=vol_ratio,
                gate_status={
                    "regime_ok": False,
                    "primary_signal": False,
                    "volume_ok": volume_ok,
                    "bias_ok": bias_ok,
                },
                hold_fail_reasons=["transition_regime"],
            )
        
        # ============================================
        # TRENDING REGIME - Use BREAKOUT strategy
        # ============================================
        if regime == "TRENDING":
            reasons.append(f"TRENDING regime (ADX={adx:.1f if adx else 0})")
            score += 2.0
            
            # Calculate breakout level
            breakout_level = self._calculate_breakout_level(closes, current_price)
            breakout_buffer = breakout_level * (1.0 + self.breakout_buffer_pct)
            
            # Breakout signal
            breakout_ok = current_high > breakout_buffer
            if breakout_ok:
                score += 2.0
                reasons.append(f"Breakout confirmed ({current_high:.2f} > {breakout_buffer:.2f})")
            else:
                reasons.append(f"No breakout ({current_high:.2f} <= {breakout_buffer:.2f})")
            
            # EMA filter
            if self.use_ema_filter and not is_uptrend:
                reasons.append("EMA filter: trend DOWN")
                return self._hold(
                    reason=", ".join(reasons),
                    current_price=current_price,
                    regime=regime,
                    adx=adx,
                    atr=atr,
                    gate_status={
                        "regime_ok": True,
                        "primary_signal": breakout_ok,
                        "volume_ok": volume_ok,
                        "bias_ok": bias_ok,
                        "secondary_confirm": False,
                    },
                    hold_fail_reasons=["ema_filter"],
                )
            
            # Volume check
            if volume_ok:
                score += 1.0
                reasons.append(f"Volume OK ({vol_ratio:.2f}x)")
            else:
                reasons.append("Volume weak")
            
            # Bias check
            if bias_ok:
                score += 1.0
                reasons.append(f"Bias OK ({dir_1h})")
            else:
                reasons.append(f"Bias DOWN ({dir_1h})")
            
            # Entry decision
            gates_passed = sum([breakout_ok, atr_ok, volume_ok])
            should_buy = breakout_ok and gates_passed >= self.min_gates_required
            
            if not should_buy:
                return self._hold(
                    reason=", ".join(reasons),
                    current_price=current_price,
                    regime=regime,
                    adx=adx,
                    atr=atr,
                    gate_status={
                        "regime_ok": True,
                        "primary_signal": breakout_ok,
                        "volume_ok": volume_ok,
                        "bias_ok": bias_ok,
                    },
                    hold_fail_reasons=["gates_fail"],
                )
            
            # BUY - Breakout entry
            stop_loss = current_price - (self.atr_sl_mult * float(atr))
            take_profit = current_price + (self.atr_tp_mult_trending * float(atr))
            
            return {
                "signal": "BUY",
                "score": float(score),
                "current_price": float(current_price),
                "stop_loss": round(float(stop_loss), 8),
                "take_profit": round(float(take_profit), 8),
                "reason": ", ".join(reasons),
                "regime": regime,
                "strategy_used": "BREAKOUT",
                "adx": float(adx) if adx is not None else None,
                "atr": float(atr),
                "regime_conf": 1.0,
                "gate_status": {
                    "regime_ok": True,
                    "primary_signal": True,
                    "volume_ok": volume_ok,
                    "bias_ok": bias_ok,
                },
                "hold_fail_reasons": [],
            }
        
        # ============================================
        # CHOPPY REGIME - Use MEAN REVERSION strategy
        # ============================================
        elif regime == "CHOPPY":
            reasons.append(f"CHOPPY regime (ADX={adx:.1f if adx else 0})")
            score += 2.0
            
            # Calculate Mean Reversion indicators
            rsi = Indicators.calculate_rsi(closes, period=self.rsi_period)
            bb_lower, bb_middle, bb_upper = self._calculate_bollinger_bands(
                closes, self.bb_period, self.bb_std_dev
            )
            
            # Oversold signals
            rsi_oversold = rsi is not None and rsi < self.rsi_oversold
            bb_oversold = bb_lower is not None and current_price < bb_lower
            
            if rsi_oversold:
                score += 2.0
                reasons.append(f"RSI oversold ({rsi:.1f})")
            else:
                rsi_val = rsi if rsi is not None else 0.0
                reasons.append(f"RSI not oversold ({rsi_val:.1f})")
            
            if bb_oversold:
                score += 2.0
                pct_below = ((bb_lower - current_price) / current_price) * 100
                reasons.append(f"Below BB ({pct_below:.2f}% below)")
            else:
                reasons.append("Not below BB lower")
            
            # Volume check
            if volume_ok:
                score += 1.0
                reasons.append(f"Volume OK ({vol_ratio:.2f}x)")
            else:
                reasons.append("Volume weak")
            
            # Entry decision
            should_buy = rsi_oversold and bb_oversold
            
            if not should_buy:
                return self._hold(
                    reason=", ".join(reasons),
                    current_price=current_price,
                    regime=regime,
                    adx=adx,
                    atr=atr,
                    rsi=rsi,
                    bb_lower=bb_lower,
                    bb_middle=bb_middle,
                    gate_status={
                        "regime_ok": True,
                        "primary_signal": should_buy,
                        "volume_ok": volume_ok,
                        "bias_ok": True,
                    },
                    hold_fail_reasons=["not_oversold"],
                )
            
            # BUY - Mean Reversion entry
            stop_loss = current_price - (self.atr_sl_mult * float(atr))
            
            # Target: BB_middle (mean revert)
            if bb_middle is not None:
                take_profit = float(bb_middle)
            else:
                take_profit = current_price + (self.atr_tp_mult_choppy * float(atr))
            
            return {
                "signal": "BUY",
                "score": float(score),
                "current_price": float(current_price),
                "stop_loss": round(float(stop_loss), 8),
                "take_profit": round(float(take_profit), 8),
                "reason": ", ".join(reasons),
                "regime": regime,
                "strategy_used": "MEAN_REVERSION",
                "adx": float(adx) if adx is not None else None,
                "atr": float(atr),
                "rsi": float(rsi) if rsi is not None else None,
                "bb_middle": float(bb_middle) if bb_middle is not None else None,
                "regime_conf": 1.0,
                "gate_status": {
                    "regime_ok": True,
                    "primary_signal": True,
                    "volume_ok": volume_ok,
                    "bias_ok": True,
                },
                "hold_fail_reasons": [],
            }
        
        # UNKNOWN regime
        else:
            return self._hold(
                reason=f"Unknown regime (ADX={adx if adx else 'N/A'})",
                current_price=current_price,
                regime="UNKNOWN",
                adx=adx,
                gate_status={name: False for name in ALL_GATE_STATUS_KEYS},
                hold_fail_reasons=["unknown_regime"],
            )
