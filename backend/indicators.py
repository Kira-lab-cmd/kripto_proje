# backend/indicators.py
import numpy as np
import pandas as pd
from typing import Optional, Dict, List
import logging

logger = logging.getLogger(__name__)


class Indicators:
    """
    TA-Lib bağımlılığı olmadan çalışan teknik indikatörler.
    Production ve deployment için güvenlidir.
    """

    @staticmethod
    def calculate_rsi(closes: List[float], period: int = 14) -> Optional[float]:
        if not closes or len(closes) < period + 1:
            return None

        try:
            s = pd.Series(closes, dtype="float64")
            delta = s.diff()

            gain = delta.clip(lower=0).rolling(period).mean()
            loss = (-delta.clip(upper=0)).rolling(period).mean()

            rs = gain.iloc[-1] / (loss.iloc[-1] if loss.iloc[-1] != 0 else 1e-9)
            rsi = 100 - (100 / (1 + rs))
            return float(rsi)
        except Exception as e:
            logger.error(f"RSI hesaplama hatası: {e}")
            return None

    @staticmethod
    def calculate_macd(
        closes: List[float],
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
    ) -> Dict[str, Optional[float]]:
        min_len = slow + signal + 1
        if not closes or len(closes) < min_len:
            return {"macd": None, "signal": None, "histogram": None}

        try:
            s = pd.Series(closes, dtype="float64")
            ema_fast = s.ewm(span=fast, adjust=False).mean()
            ema_slow = s.ewm(span=slow, adjust=False).mean()

            macd_line = ema_fast - ema_slow
            signal_line = macd_line.ewm(span=signal, adjust=False).mean()
            hist = macd_line - signal_line

            return {
                "macd": float(macd_line.iloc[-1]),
                "signal": float(signal_line.iloc[-1]),
                "histogram": float(hist.iloc[-1]),
            }
        except Exception as e:
            logger.error(f"MACD hesaplama hatası: {e}")
            return {"macd": None, "signal": None, "histogram": None}

    @staticmethod
    def calculate_bollinger(
        closes: List[float],
        period: int = 20,
        dev: float = 2.0,
    ) -> Dict[str, Optional[float]]:
        if not closes or len(closes) < period:
            return {"upper": None, "middle": None, "lower": None}

        try:
            s = pd.Series(closes, dtype="float64")
            mid = s.rolling(period).mean().iloc[-1]
            std = s.rolling(period).std().iloc[-1]

            upper = mid + dev * std
            lower = mid - dev * std

            return {
                "upper": float(upper),
                "middle": float(mid),
                "lower": float(lower),
            }
        except Exception as e:
            logger.error(f"Bollinger hesaplama hatası: {e}")
            return {"upper": None, "middle": None, "lower": None}

    @staticmethod
    def calculate_atr(
        highs: List[float],
        lows: List[float],
        closes: List[float],
        period: int = 14,
    ) -> Optional[float]:
        min_len = period + 1
        if (
            not highs
            or len(highs) < min_len
            or len(lows) != len(highs)
            or len(closes) != len(highs)
        ):
            return None

        try:
            h = pd.Series(highs, dtype="float64")
            l = pd.Series(lows, dtype="float64")
            c = pd.Series(closes, dtype="float64")

            prev_close = c.shift(1)
            tr = pd.concat(
                [
                    (h - l).abs(),
                    (h - prev_close).abs(),
                    (l - prev_close).abs(),
                ],
                axis=1,
            ).max(axis=1)

            atr = tr.rolling(period).mean().iloc[-1]
            return float(atr)
        except Exception as e:
            logger.error(f"ATR hesaplama hatası: {e}")
            return None

    @staticmethod
    def calculate_adx(
        highs: List[float],
        lows: List[float],
        closes: List[float],
        period: int = 14
    ) -> Optional[float]:
        """
        Calculate Average Directional Index (ADX)
        
        ADX measures trend strength (0-100):
        - ADX < 20: Weak/no trend (choppy)
        - ADX 20-25: Emerging trend
        - ADX > 25: Strong trend
        - ADX > 50: Very strong trend
        """
        min_len = period * 2 + 1
        if (
            not highs
            or not lows
            or not closes
            or len(highs) < min_len
            or len(lows) != len(highs)
            or len(closes) != len(highs)
        ):
            return None

        try:
            h = pd.Series(highs, dtype="float64")
            l = pd.Series(lows, dtype="float64")
            c = pd.Series(closes, dtype="float64")

            # Calculate +DM and -DM (Directional Movement)
            high_diff = h.diff()
            low_diff = -l.diff()
            
            plus_dm = pd.Series(0.0, index=h.index)
            minus_dm = pd.Series(0.0, index=h.index)
            
            # +DM when high_diff > low_diff and high_diff > 0
            plus_dm[(high_diff > low_diff) & (high_diff > 0)] = high_diff
            
            # -DM when low_diff > high_diff and low_diff > 0
            minus_dm[(low_diff > high_diff) & (low_diff > 0)] = low_diff

            # Calculate True Range
            prev_close = c.shift(1)
            tr = pd.concat(
                [
                    (h - l).abs(),
                    (h - prev_close).abs(),
                    (l - prev_close).abs(),
                ],
                axis=1,
            ).max(axis=1)

            # Smooth +DM, -DM, and TR using Wilder's smoothing (EMA)
            atr = tr.ewm(alpha=1/period, adjust=False).mean()
            plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / atr)
            minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / atr)

            # Calculate DX (Directional Index)
            dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
            
            # Replace inf/nan with 0
            dx = dx.replace([np.inf, -np.inf], 0).fillna(0)

            # Calculate ADX (smoothed DX)
            adx = dx.ewm(alpha=1/period, adjust=False).mean().iloc[-1]
            
            return float(adx)
        except Exception as e:
            logger.error(f"ADX hesaplama hatası: {e}")
            return None

    @staticmethod
    def from_ohlcv(ohlcv: List[List[float]]) -> Dict:
        """
        ohlcv: [[ts, open, high, low, close, volume], ...]
        """
        if not ohlcv or len(ohlcv[0]) < 6:
            return {"rsi": None, "macd": None, "bollinger": None, "atr": None, "adx": None}

        highs = [c[2] for c in ohlcv]
        lows = [c[3] for c in ohlcv]
        closes = [c[4] for c in ohlcv]

        return {
            "rsi": Indicators.calculate_rsi(closes),
            "macd": Indicators.calculate_macd(closes),
            "bollinger": Indicators.calculate_bollinger(closes),
            "atr": Indicators.calculate_atr(highs, lows, closes),
            "adx": Indicators.calculate_adx(highs, lows, closes),
        }
