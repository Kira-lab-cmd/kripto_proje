from __future__ import annotations

import logging
import os
from typing import Any, Dict

import pandas as pd

from .indicators import Indicators
from .utils_symbols import normalize_symbol

logger = logging.getLogger(__name__)


class SnapbackStrategy:
    """
    PAPER MODE SNAPBACK LONG STRATEGY

    Amaç:
    - Breakout yokken kısa vadeli aşağı sarkmaları yakalamak
    - Tepki hareketine girmek
    - Küçük TP / kontrollü SL ile çıkmak

    BUY mantığı:
    - 1H bias DOWN değil
    - ATR trade edilebilir aralıkta
    - kısa vadede aşağı baskı var
    - son mum toparlanma gösteriyor
    - fiyat kısa EMA'nın altında ama aşırı bozulmuş değil
    """

    def __init__(self) -> None:
        self.pullback_lookback = int(os.getenv("SNAPBACK_PULLBACK_LOOKBACK", "4"))
        self.min_drop_pct = float(os.getenv("SNAPBACK_MIN_DROP_PCT", "0.006"))  # %0.6
        self.min_reclaim_pct = float(os.getenv("SNAPBACK_MIN_RECLAIM_PCT", "0.0015"))  # %0.15

        self.min_atr_pct = float(os.getenv("SNAPBACK_MIN_ATR_PCT", "0.003"))
        self.max_atr_pct = float(os.getenv("SNAPBACK_MAX_ATR_PCT", "0.03"))

        self.ema_fast_len = int(os.getenv("SNAPBACK_EMA_FAST_LEN", "20"))
        self.ema_fast_tolerance_mult = float(os.getenv("SNAPBACK_EMA_FAST_TOLERANCE_MULT", "1.0015"))

        self.atr_sl_mult = float(os.getenv("SNAPBACK_ATR_SL_MULT", "1.2"))
        self.atr_tp_mult = float(os.getenv("SNAPBACK_ATR_TP_MULT", "1.8"))

    @staticmethod
    def _ema(values: list[float], period: int) -> float | None:
        if not values or len(values) < period:
            return None
        try:
            s = pd.Series(values, dtype="float64")
            return float(s.ewm(span=period, adjust=False).mean().iloc[-1])
        except Exception:
            return None

    def get_config_snapshot(self) -> Dict[str, Any]:
        return {
            "pullback_lookback": self.pullback_lookback,
            "min_drop_pct": self.min_drop_pct,
            "min_reclaim_pct": self.min_reclaim_pct,
            "min_atr_pct": self.min_atr_pct,
            "max_atr_pct": self.max_atr_pct,
            "ema_fast_len": self.ema_fast_len,
            "ema_fast_tolerance_mult": self.ema_fast_tolerance_mult,
            "atr_sl_mult": self.atr_sl_mult,
            "atr_tp_mult": self.atr_tp_mult,
        }

    def _log_gate_probe(
        self,
        *,
        symbol: str | None,
        current_price: float,
        ema_fast: float | None,
        tolerance_ceiling: float | None,
        below_ema_fast: bool,
    ) -> None:
        logger.debug(
            "snapback_gate_probe symbol=%s price=%.8f ema_fast=%s tolerance_mult=%.6f tolerance_ceiling=%s below_ema_fast=%s",
            normalize_symbol(symbol or "") or (symbol or ""),
            float(current_price),
            f"{float(ema_fast):.8f}" if ema_fast is not None else "None",
            float(self.ema_fast_tolerance_mult),
            f"{float(tolerance_ceiling):.8f}" if tolerance_ceiling is not None else "None",
            str(bool(below_ema_fast)).lower(),
        )

    def _hold(
        self,
        *,
        reason: str,
        current_price: float | None,
        atr: float | None,
        atr_pct: float | None,
        ema_fast: float | None,
        trend_dir_1h: str | None,
        gate_status: Dict[str, bool],
        hold_fail_reasons: list[str],
        score: float = 0.0,
    ) -> Dict[str, Any]:
        return {
            "signal": "HOLD",
            "score": float(score),
            "current_price": current_price,
            "stop_loss": None,
            "take_profit": None,
            "reason": reason,
            "is_uptrend": bool(gate_status.get("bias_not_down", False)),
            "ema200": None,
            "atr": float(atr) if atr is not None else None,
            "atr_pct": float(atr_pct) if atr_pct is not None else None,
            "vol_ratio": None,
            "regime": "SNAPBACK_NO_SETUP",
            "regime_conf": 0.0,
            "adx": None,
            "er": None,
            "dir_1h": (trend_dir_1h or "UNKNOWN").upper(),
            "base_thresholds": {"buy": 4.0, "sell": -999.0},
            "threshold_bias": {"model": "snapback_v1"},
            "effective_thresholds": {"buy": 4.0, "sell": -999.0},
            "gate_status": gate_status,
            "hold_fail_reasons": hold_fail_reasons,
            "strategy_name": "snapback",
            "ema_fast": round(float(ema_fast), 8) if ema_fast is not None else None,
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
        if not ohlcv_data or len(ohlcv_data) < 50:
            return self._hold(
                reason="Yetersiz veri",
                current_price=None,
                atr=None,
                atr_pct=None,
                ema_fast=None,
                trend_dir_1h=trend_dir_1h,
                gate_status={
                    "bias_not_down": False,
                    "atr_ok": False,
                    "pullback_ok": False,
                    "reclaim_ok": False,
                    "below_ema_fast": False,
                },
                hold_fail_reasons=[
                    "bias_not_down",
                    "atr_ok",
                    "pullback_ok",
                    "reclaim_ok",
                    "below_ema_fast",
                ],
                score=0.0,
            )

        closes = [float(c[4]) for c in ohlcv_data]
        highs = [float(c[2]) for c in ohlcv_data]
        lows = [float(c[3]) for c in ohlcv_data]

        current_price = float(closes[-1])
        previous_close = float(closes[-2])

        ema_fast = self._ema(closes, self.ema_fast_len)

        atr = Indicators.calculate_atr(highs, lows, closes)
        atr_pct = (float(atr) / current_price) if atr and current_price > 0 else None

        dir_1h = (trend_dir_1h or "UNKNOWN").upper()
        bias_not_down = dir_1h in {"UP", "NEUTRAL"}

        atr_ok = atr_pct is not None and self.min_atr_pct <= atr_pct <= self.max_atr_pct

        lb = max(3, self.pullback_lookback)
        recent_peak = max(closes[-lb - 1:-1])
        drop_from_peak_pct = (recent_peak - current_price) / recent_peak if recent_peak > 0 else 0.0
        pullback_ok = drop_from_peak_pct >= self.min_drop_pct

        reclaim_pct = (current_price - previous_close) / previous_close if previous_close > 0 else 0.0
        reclaim_ok = reclaim_pct >= self.min_reclaim_pct

        tolerance_ceiling = (float(ema_fast) * self.ema_fast_tolerance_mult) if ema_fast is not None else None
        below_ema_fast = bool(tolerance_ceiling is not None and current_price <= tolerance_ceiling)
        self._log_gate_probe(
            symbol=symbol,
            current_price=current_price,
            ema_fast=ema_fast,
            tolerance_ceiling=tolerance_ceiling,
            below_ema_fast=below_ema_fast,
        )

        gate_status = {
            "bias_not_down": bias_not_down,
            "atr_ok": atr_ok,
            "pullback_ok": pullback_ok,
            "reclaim_ok": reclaim_ok,
            "below_ema_fast": below_ema_fast,
        }

        hold_fail_reasons = [k for k, v in gate_status.items() if not v]

        reasons: list[str] = []
        score = 0.0
        sym = normalize_symbol(symbol or "")

        if bias_not_down:
            score += 1.0
            if dir_1h == "NEUTRAL":
                reasons.append("1H bias=NEUTRAL (allowed)")
            else:
                reasons.append("1H bias=UP")
        else:
            reasons.append(f"1H bias={dir_1h}")

        if atr_ok:
            score += 1.0
            reasons.append(f"ATR% ok ({atr_pct * 100:.2f})")
        else:
            reasons.append("ATR gate fail")

        if pullback_ok:
            score += 1.0
            reasons.append(f"pullback ok ({drop_from_peak_pct * 100:.2f})")
        else:
            reasons.append("pullback yok")

        if reclaim_ok:
            score += 1.0
            reasons.append(f"reclaim ok ({reclaim_pct * 100:.2f})")
        else:
            reasons.append("reclaim yok")

        if below_ema_fast:
            score += 1.0
            reasons.append("below EMA fast")
        else:
            reasons.append("EMA fast üstünde")

        if below_ema_fast and reasons and reasons[-1] == "below EMA fast" and ema_fast is not None and current_price > float(ema_fast):
            reasons[-1] = "within EMA fast tolerance"
        elif not below_ema_fast and reasons:
            reasons[-1] = "EMA fast tolerance fail" if ema_fast is not None else "EMA fast unavailable"

        should_buy = all([
            bias_not_down,
            atr_ok,
            pullback_ok,
            reclaim_ok,
            below_ema_fast,
        ])

        if not should_buy:
            if sym:
                reasons.append(f"SYM={sym}")
            return self._hold(
                reason=", ".join(reasons),
                current_price=current_price,
                atr=atr,
                atr_pct=atr_pct,
                ema_fast=ema_fast,
                trend_dir_1h=dir_1h,
                gate_status=gate_status,
                hold_fail_reasons=hold_fail_reasons,
                score=score,
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
            "is_uptrend": bool(bias_not_down),
            "ema200": None,
            "atr": float(atr) if atr is not None else None,
            "atr_pct": float(atr_pct) if atr_pct is not None else None,
            "vol_ratio": None,
            "regime": "SNAPBACK_READY",
            "regime_conf": 1.0,
            "adx": None,
            "er": None,
            "dir_1h": dir_1h,
            "base_thresholds": {"buy": 4.0, "sell": -999.0},
            "threshold_bias": {"model": "snapback_v1"},
            "effective_thresholds": {"buy": 4.0, "sell": -999.0},
            "gate_status": gate_status,
            "hold_fail_reasons": [],
            "strategy_name": "snapback",
            "ema_fast": round(float(ema_fast), 8) if ema_fast is not None else None,
        }