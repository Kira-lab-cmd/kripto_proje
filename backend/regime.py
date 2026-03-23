# File: backend/regime.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

Regime = Literal["TREND", "CHOP", "HIGH_VOL", "UNKNOWN"]


@dataclass(frozen=True)
class RegimeConfig:
    adx_period: int = 14
    er_period: int = 30

    high_vol_atr_pct: float = 0.060  # 6%

    trend_adx_min: float = 23.0
    trend_er_min: float = 0.45

    chop_adx_max: float = 18.0
    chop_er_max: float = 0.25


@dataclass(frozen=True)
class RegimeResult:
    regime: Regime
    adx: float | None
    er: float | None
    atr_pct: float | None
    confidence: float  # 0..1
    reason: str


class RegimeDetector:
    def __init__(self, cfg: RegimeConfig | None = None) -> None:
        self.cfg = cfg or RegimeConfig()

    @staticmethod
    def _ensure_series(values: list[float]) -> pd.Series:
        return pd.Series([float(v) for v in values], dtype="float64")

    def _atr(self, highs: list[float], lows: list[float], closes: list[float], period: int) -> float | None:
        if len(highs) < period + 1 or len(lows) != len(highs) or len(closes) != len(highs):
            return None
        h = self._ensure_series(highs)
        l = self._ensure_series(lows)
        c = self._ensure_series(closes)
        prev_close = c.shift(1)
        tr = pd.concat([(h - l).abs(), (h - prev_close).abs(), (l - prev_close).abs()], axis=1).max(axis=1)
        atr = tr.rolling(period).mean().iloc[-1]
        if pd.isna(atr):
            return None
        return float(atr)

    def _adx(self, highs: list[float], lows: list[float], closes: list[float], period: int) -> float | None:
        if len(highs) < (period * 3) or len(lows) != len(highs) or len(closes) != len(highs):
            return None

        h = self._ensure_series(highs)
        l = self._ensure_series(lows)
        c = self._ensure_series(closes)

        up_move = h.diff()
        down_move = -l.diff()

        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        prev_close = c.shift(1)
        tr = pd.concat([(h - l).abs(), (h - prev_close).abs(), (l - prev_close).abs()], axis=1).max(axis=1)

        atr = tr.rolling(period).mean()
        plus_di = 100.0 * (plus_dm.rolling(period).mean() / atr.replace(0.0, 1e-12))
        minus_di = 100.0 * (minus_dm.rolling(period).mean() / atr.replace(0.0, 1e-12))

        dx = (100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, 1e-12))
        adx = dx.rolling(period).mean().iloc[-1]
        if pd.isna(adx):
            return None
        return float(adx)

    def _er(self, closes: list[float], period: int) -> float | None:
        """
        Kaufman Efficiency Ratio:
          ER = abs(close_t - close_{t-period}) / sum(abs(diff(close)) over period)
        Range: [0,1]
        """
        if len(closes) < period + 1:
            return None
        c = self._ensure_series(closes)

        change = abs(float(c.iloc[-1]) - float(c.iloc[-(period + 1)]))
        volatility = float(c.diff().abs().iloc[-period:].sum())
        denom = volatility if volatility > 0 else 1e-12
        er = change / denom
        er = max(0.0, min(1.0, er))
        return float(er)

    def detect(self, highs: list[float], lows: list[float], closes: list[float]) -> RegimeResult:
        cfg = self.cfg

        adx = self._adx(highs, lows, closes, cfg.adx_period)
        er = self._er(closes, cfg.er_period)

        atr = self._atr(highs, lows, closes, period=cfg.adx_period)
        last_close = float(closes[-1]) if closes else 0.0
        atr_pct = (float(atr) / last_close) if atr and last_close > 0 else None

        if atr_pct is not None and atr_pct >= cfg.high_vol_atr_pct:
            return RegimeResult(
                regime="HIGH_VOL",
                adx=adx,
                er=er,
                atr_pct=atr_pct,
                confidence=0.85,
                reason=f"HIGH_VOL (ATR%={atr_pct*100:.2f} >= {cfg.high_vol_atr_pct*100:.2f})",
            )

        if adx is not None and er is not None:
            if adx >= cfg.trend_adx_min and er >= cfg.trend_er_min:
                conf = max(0.55, min(0.95, 0.55 + (adx - cfg.trend_adx_min) / 50.0 + (er - cfg.trend_er_min)))
                return RegimeResult(
                    regime="TREND",
                    adx=adx,
                    er=er,
                    atr_pct=atr_pct,
                    confidence=conf,
                    reason=f"TREND (ADX={adx:.1f}, ER={er:.2f})",
                )

            if adx <= cfg.chop_adx_max and er <= cfg.chop_er_max:
                conf = max(0.55, min(0.95, 0.55 + (cfg.chop_adx_max - adx) / 30.0 + (cfg.chop_er_max - er)))
                return RegimeResult(
                    regime="CHOP",
                    adx=adx,
                    er=er,
                    atr_pct=atr_pct,
                    confidence=conf,
                    reason=f"CHOP (ADX={adx:.1f}, ER={er:.2f})",
                )

        return RegimeResult(
            regime="UNKNOWN",
            adx=adx,
            er=er,
            atr_pct=atr_pct,
            confidence=0.35,
            reason="UNKNOWN (no strong regime separation)",
        )