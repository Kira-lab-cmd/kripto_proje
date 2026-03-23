# backend/strategy/signal_engine.py
"""
Profesyonel çok katmanlı sinyal motoru.
Confluence tabanlı: birden fazla indikatör aynı yönü göstermeli.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class SignalResult:
    signal: str          # BUY / SELL / HOLD
    score: float         # -10 ile +10 arası
    confidence: float    # 0.0 - 1.0
    reason: str
    components: dict = field(default_factory=dict)
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    risk_reward: Optional[float] = None
    strategy_name: str = "confluence_v2"


class IndicatorEngine:
    """Tüm teknik indikatörleri hesaplar."""

    @staticmethod
    def rsi(closes: np.ndarray, period: int = 14) -> np.ndarray:
        delta = np.diff(closes)
        gain = np.where(delta > 0, delta, 0.0)
        loss = np.where(delta < 0, -delta, 0.0)

        avg_gain = np.zeros(len(closes))
        avg_loss = np.zeros(len(closes))

        avg_gain[period] = np.mean(gain[:period])
        avg_loss[period] = np.mean(loss[:period])

        for i in range(period + 1, len(closes)):
            avg_gain[i] = (avg_gain[i-1] * (period-1) + gain[i-1]) / period
            avg_loss[i] = (avg_loss[i-1] * (period-1) + loss[i-1]) / period

        rs = np.where(avg_loss == 0, 100.0, avg_gain / avg_loss)
        rsi = 100 - (100 / (1 + rs))
        rsi[:period] = 50.0
        return rsi

    @staticmethod
    def ema(closes: np.ndarray, period: int) -> np.ndarray:
        alpha = 2.0 / (period + 1)
        ema = np.zeros(len(closes))
        ema[0] = closes[0]
        for i in range(1, len(closes)):
            ema[i] = alpha * closes[i] + (1 - alpha) * ema[i-1]
        return ema

    @staticmethod
    def macd(closes: np.ndarray,
             fast: int = 12,
             slow: int = 26,
             signal: int = 9) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        ema_fast = IndicatorEngine.ema(closes, fast)
        ema_slow = IndicatorEngine.ema(closes, slow)
        macd_line = ema_fast - ema_slow
        signal_line = IndicatorEngine.ema(macd_line, signal)
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def bollinger_bands(closes: np.ndarray,
                        period: int = 20,
                        std_dev: float = 2.0) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        upper = np.zeros(len(closes))
        middle = np.zeros(len(closes))
        lower = np.zeros(len(closes))

        for i in range(period - 1, len(closes)):
            window = closes[i - period + 1:i + 1]
            mid = np.mean(window)
            std = np.std(window, ddof=1)
            middle[i] = mid
            upper[i] = mid + std_dev * std
            lower[i] = mid - std_dev * std

        return upper, middle, lower

    @staticmethod
    def atr(highs: np.ndarray,
            lows: np.ndarray,
            closes: np.ndarray,
            period: int = 14) -> np.ndarray:
        tr = np.maximum(
            highs - lows,
            np.maximum(
                np.abs(highs - np.roll(closes, 1)),
                np.abs(lows - np.roll(closes, 1))
            )
        )
        tr[0] = highs[0] - lows[0]
        atr = np.zeros(len(closes))
        atr[period-1] = np.mean(tr[:period])
        for i in range(period, len(closes)):
            atr[i] = (atr[i-1] * (period-1) + tr[i]) / period
        return atr

    @staticmethod
    def volume_ratio(volumes: np.ndarray, period: int = 20) -> np.ndarray:
        """Mevcut hacim / ortalama hacim."""
        ratio = np.ones(len(volumes))
        for i in range(period, len(volumes)):
            avg = np.mean(volumes[i-period:i])
            ratio[i] = volumes[i] / avg if avg > 0 else 1.0
        return ratio

    @staticmethod
    def stochastic_rsi(closes: np.ndarray,
                       rsi_period: int = 14,
                       stoch_period: int = 14,
                       k_period: int = 3,
                       d_period: int = 3) -> tuple[np.ndarray, np.ndarray]:
        rsi = IndicatorEngine.rsi(closes, rsi_period)
        stoch_k = np.zeros(len(closes))

        for i in range(stoch_period - 1, len(closes)):
            window = rsi[i - stoch_period + 1:i + 1]
            min_rsi = np.min(window)
            max_rsi = np.max(window)
            if max_rsi - min_rsi > 0:
                stoch_k[i] = (rsi[i] - min_rsi) / (max_rsi - min_rsi) * 100
            else:
                stoch_k[i] = 50.0

        # Smooth K
        smooth_k = np.zeros(len(closes))
        for i in range(k_period - 1, len(closes)):
            smooth_k[i] = np.mean(stoch_k[i - k_period + 1:i + 1])

        # D line
        stoch_d = np.zeros(len(closes))
        for i in range(d_period - 1, len(closes)):
            stoch_d[i] = np.mean(smooth_k[i - d_period + 1:i + 1])

        return smooth_k, stoch_d

    @staticmethod
    def adx(highs: np.ndarray,
            lows: np.ndarray,
            closes: np.ndarray,
            period: int = 14) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """ADX, +DI, -DI döndürür."""
        n = len(closes)
        plus_dm = np.zeros(n)
        minus_dm = np.zeros(n)
        tr = np.zeros(n)

        for i in range(1, n):
            h_diff = highs[i] - highs[i-1]
            l_diff = lows[i-1] - lows[i]
            plus_dm[i] = h_diff if (h_diff > l_diff and h_diff > 0) else 0
            minus_dm[i] = l_diff if (l_diff > h_diff and l_diff > 0) else 0
            tr[i] = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )

        # Smoothed
        atr_s = np.zeros(n)
        plus_di = np.zeros(n)
        minus_di = np.zeros(n)
        adx_vals = np.zeros(n)

        atr_s[period] = np.sum(tr[1:period+1])
        pdm_s = np.sum(plus_dm[1:period+1])
        mdm_s = np.sum(minus_dm[1:period+1])

        for i in range(period + 1, n):
            atr_s[i] = atr_s[i-1] - atr_s[i-1]/period + tr[i]
            pdm_s = pdm_s - pdm_s/period + plus_dm[i]
            mdm_s = mdm_s - mdm_s/period + minus_dm[i]

            plus_di[i] = 100 * pdm_s / atr_s[i] if atr_s[i] > 0 else 0
            minus_di[i] = 100 * mdm_s / atr_s[i] if atr_s[i] > 0 else 0

            dx = 100 * abs(plus_di[i] - minus_di[i]) / (plus_di[i] + minus_di[i]) \
                if (plus_di[i] + minus_di[i]) > 0 else 0

            if i == period * 2:
                adx_vals[i] = dx
            elif i > period * 2:
                adx_vals[i] = (adx_vals[i-1] * (period-1) + dx) / period

        return adx_vals, plus_di, minus_di

    @staticmethod
    def supertrend(highs: np.ndarray,
                   lows: np.ndarray,
                   closes: np.ndarray,
                   period: int = 10,
                   multiplier: float = 3.0) -> tuple[np.ndarray, np.ndarray]:
        """SuperTrend indikatörü. direction: 1=UP, -1=DOWN"""
        atr = IndicatorEngine.atr(highs, lows, closes, period)
        n = len(closes)
        hl2 = (highs + lows) / 2

        upper_band = hl2 + multiplier * atr
        lower_band = hl2 - multiplier * atr

        supertrend = np.zeros(n)
        direction = np.ones(n)  # 1=bullish, -1=bearish

        for i in range(1, n):
            # Upper band
            if upper_band[i] < upper_band[i-1] or closes[i-1] > upper_band[i-1]:
                upper_band[i] = upper_band[i]
            else:
                upper_band[i] = upper_band[i-1]

            # Lower band
            if lower_band[i] > lower_band[i-1] or closes[i-1] < lower_band[i-1]:
                lower_band[i] = lower_band[i]
            else:
                lower_band[i] = lower_band[i-1]

            # Direction
            if supertrend[i-1] == upper_band[i-1]:
                direction[i] = -1 if closes[i] > upper_band[i] else 1
            else:
                direction[i] = 1 if closes[i] < lower_band[i] else -1

            supertrend[i] = lower_band[i] if direction[i] == -1 else upper_band[i]

        return supertrend, direction


class ConfluenceSignalEngine:
    """
    Profesyonel confluence tabanlı sinyal motoru.
    
    Kar etmek için: birden fazla indikatör aynı yönü göstermeli.
    Her indikatör oy kullanır, çoğunluk kazanır.
    """

    def __init__(self, config: dict | None = None):
        self.cfg = config or {}
        
        # Ağırlıklar (toplam = 10)
        self.weights = {
            "trend":      self.cfg.get("w_trend", 2.5),    # EMA trend
            "momentum":   self.cfg.get("w_momentum", 2.0), # RSI + StochRSI
            "macd":       self.cfg.get("w_macd", 2.0),     # MACD
            "volume":     self.cfg.get("w_volume", 1.5),   # Volume confirmation
            "supertrend": self.cfg.get("w_supertrend", 2.0), # SuperTrend
        }
        
        # Eşikler
        self.buy_threshold = float(self.cfg.get("buy_threshold", 4.5))
        self.sell_threshold = float(self.cfg.get("sell_threshold", -4.5))
        self.min_atr_pct = float(self.cfg.get("min_atr_pct", 0.003))
        self.max_atr_pct = float(self.cfg.get("max_atr_pct", 0.08))
        
        # Risk/Reward
        self.sl_atr_mult = float(self.cfg.get("sl_atr_mult", 1.5))
        self.tp_atr_mult = float(self.cfg.get("tp_atr_mult", 3.0))
        self.min_rr = float(self.cfg.get("min_rr", 1.8))

    def compute(self,
                ohlcv: list,
                symbol: str = "",
                sentiment_score: float = 0.0,
                trend_dir_1h: str = "UNKNOWN",
                profile: dict | None = None) -> SignalResult:
        """
        Ana sinyal hesaplama fonksiyonu.
        
        ohlcv: [[timestamp, open, high, low, close, volume], ...]
        """
        if not ohlcv or len(ohlcv) < 50:
            return SignalResult(
                signal="HOLD",
                score=0.0,
                confidence=0.0,
                reason="insufficient_data",
            )

        try:
            arr = np.array(ohlcv, dtype=float)
            opens   = arr[:, 1]
            highs   = arr[:, 2]
            lows    = arr[:, 3]
            closes  = arr[:, 4]
            volumes = arr[:, 5]
        except Exception as