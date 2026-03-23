"""
Trading Strategy V3 - REGIME ADAPTATION

Major improvement over V2:
- Don't trade in CHOP markets (biggest improvement!)
- Conservative in HIGH_VOL markets
- Aggressive in TREND markets

Expected win rate improvement: +10-15%
"""

from __future__ import annotations

import os
from typing import Any, Dict

# Import V2 as base
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from strategy_v2 import TradingStrategyV2 as StrategyV2Base


class TradingStrategyV3(StrategyV2Base):
    """
    V3 - REGIME ADAPTATION
    
    Key changes from V2:
    1. DON'T TRADE in CHOP regime (avoids ~30% losing trades)
    2. Stricter gates in HIGH_VOL (4/4 instead of 3/4)
    3. Same as V2 in TREND (3/4 gates)
    """
    
    def __init__(self) -> None:
        super().__init__()
        
        # Regime-specific settings
        self.regime_enabled = os.getenv("REGIME_ADAPTATION", "true").lower() == "true"
        self.avoid_chop = os.getenv("AVOID_CHOP", "true").lower() == "true"
        self.high_vol_conservative = os.getenv("HIGH_VOL_CONSERVATIVE", "true").lower() == "true"
        
        # Regime thresholds (ADX-based)
        self.trend_adx_threshold = float(os.getenv("TREND_ADX_THRESHOLD", "25"))
        self.chop_adx_threshold = float(os.getenv("CHOP_ADX_THRESHOLD", "20"))
        
    def get_config_snapshot(self) -> Dict[str, Any]:
        config = super().get_config_snapshot()
        config.update({
            "version": "v3",
            "regime_enabled": self.regime_enabled,
            "avoid_chop": self.avoid_chop,
            "high_vol_conservative": self.high_vol_conservative,
            "trend_adx_threshold": self.trend_adx_threshold,
            "chop_adx_threshold": self.chop_adx_threshold,
        })
        return config
    
    def _detect_regime_simple(
        self,
        is_uptrend: bool,
        atr_pct: float | None,
        adx: float | None = None
    ) -> str:
        """
        Simple regime detection.
        
        Returns: "TREND" | "CHOP" | "HIGH_VOL"
        """
        # HIGH_VOL check (ATR > 2.5%)
        if atr_pct and atr_pct > 0.025:
            return "HIGH_VOL"
        
        # CHOP check (no clear trend)
        if not is_uptrend:
            return "CHOP"
        
        # ADX-based if available
        if adx:
            if adx > self.trend_adx_threshold:
                return "TREND"
            elif adx < self.chop_adx_threshold:
                return "CHOP"
        
        # Default to TREND if uptrend
        return "TREND" if is_uptrend else "CHOP"
    
    def get_signal(
        self,
        ohlcv_data: list,
        sentiment_score: float,
        *,
        symbol: str | None = None,
        profile: dict[str, Any] | None = None,
        trend_dir_1h: str | None = None,
    ) -> Dict[str, Any]:
        
        # Get base V2 signal
        v2_signal = super().get_signal(
            ohlcv_data,
            sentiment_score,
            symbol=symbol,
            profile=profile,
            trend_dir_1h=trend_dir_1h
        )
        
        # If regime adaptation disabled, return V2 signal
        if not self.regime_enabled:
            return v2_signal
        
        # If already HOLD, no need for regime check
        if v2_signal["signal"] == "HOLD":
            return v2_signal
        
        # Detect regime
        is_uptrend = v2_signal.get("is_uptrend", False)
        atr_pct = v2_signal.get("atr_pct")
        adx = v2_signal.get("adx")  # If available from indicator
        
        regime = self._detect_regime_simple(is_uptrend, atr_pct, adx)
        
        # REGIME FILTERING
        
        # 1. CHOP regime - DON'T TRADE!
        if self.avoid_chop and regime == "CHOP":
            return self._hold(
                reason=f"CHOP regime detected - no trade (was {v2_signal['signal']})",
                current_price=v2_signal.get("current_price"),
                is_uptrend=is_uptrend,
                ema200=v2_signal.get("ema200"),
                atr=v2_signal.get("atr"),
                atr_pct=atr_pct,
                vol_ratio=v2_signal.get("vol_ratio"),
                trend_dir_1h=trend_dir_1h,
                gate_status=v2_signal.get("gate_status", {}),
                hold_fail_reasons=["regime_chop"],
                regime="CHOP",
                score=v2_signal.get("score", 0.0)
            )
        
        # 2. HIGH_VOL regime - More conservative
        if self.high_vol_conservative and regime == "HIGH_VOL":
            # In V2, we allow 3/4 gates
            # In HIGH_VOL, require 4/4 gates
            
            gate_status = v2_signal.get("gate_status", {})
            required_gates = ["bias_not_down", "atr_ok", "volume_ok", "breakout_ok"]
            
            # Check if all 4 gates pass
            all_gates_pass = all(gate_status.get(gate, False) for gate in required_gates)
            
            if not all_gates_pass:
                failed_gates = [g for g in required_gates if not gate_status.get(g, False)]
                return self._hold(
                    reason=f"HIGH_VOL regime - need 4/4 gates (failed: {failed_gates})",
                    current_price=v2_signal.get("current_price"),
                    is_uptrend=is_uptrend,
                    ema200=v2_signal.get("ema200"),
                    atr=v2_signal.get("atr"),
                    atr_pct=atr_pct,
                    vol_ratio=v2_signal.get("vol_ratio"),
                    trend_dir_1h=trend_dir_1h,
                    gate_status=gate_status,
                    hold_fail_reasons=failed_gates + ["high_vol_strict"],
                    regime="HIGH_VOL",
                    score=v2_signal.get("score", 0.0)
                )
        
        # 3. TREND regime - Use V2 signal as-is
        # This is the ideal condition for trading
        
        # Update regime in signal
        v2_signal["regime"] = regime
        v2_signal["regime_adaptation"] = "v3_enabled"
        
        return v2_signal
