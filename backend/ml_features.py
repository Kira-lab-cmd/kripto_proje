"""
Machine Learning Feature Extraction

Extracts comprehensive features from OHLCV data for ML model.
"""

from typing import Dict, List, Optional
import pandas as pd
import numpy as np


class MLFeatureExtractor:
    """Extract features for ML trading model"""
    
    @staticmethod
    def _ema(values: List[float], period: int) -> Optional[float]:
        """Calculate EMA"""
        if not values or len(values) < period:
            return None
        try:
            s = pd.Series(values, dtype="float64")
            return float(s.ewm(span=period, adjust=False).mean().iloc[-1])
        except Exception:
            return None
    
    @staticmethod
    def _rsi(closes: List[float], period: int = 14) -> Optional[float]:
        """Calculate RSI"""
        if not closes or len(closes) < period + 1:
            return None
        try:
            deltas = pd.Series(closes).diff()
            gain = deltas.where(deltas > 0, 0).rolling(window=period).mean()
            loss = -deltas.where(deltas < 0, 0).rolling(window=period).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return float(rsi.iloc[-1])
        except Exception:
            return None
    
    @staticmethod
    def _atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
        """Calculate ATR"""
        if not highs or len(highs) < period + 1:
            return None
        try:
            h = pd.Series(highs)
            l = pd.Series(lows)
            c = pd.Series(closes)
            prev_close = c.shift(1)
            tr = pd.concat([
                (h - l).abs(),
                (h - prev_close).abs(),
                (l - prev_close).abs(),
            ], axis=1).max(axis=1)
            atr = tr.rolling(period).mean().iloc[-1]
            return float(atr)
        except Exception:
            return None
    
    @staticmethod
    def _adx(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
        """Calculate ADX"""
        if not highs or len(highs) < period * 2 + 1:
            return None
        try:
            h = pd.Series(highs)
            l = pd.Series(lows)
            c = pd.Series(closes)
            
            high_diff = h.diff()
            low_diff = -l.diff()
            
            plus_dm = pd.Series(0.0, index=h.index)
            minus_dm = pd.Series(0.0, index=h.index)
            
            plus_dm[(high_diff > low_diff) & (high_diff > 0)] = high_diff
            minus_dm[(low_diff > high_diff) & (low_diff > 0)] = low_diff
            
            prev_close = c.shift(1)
            tr = pd.concat([
                (h - l).abs(),
                (h - prev_close).abs(),
                (l - prev_close).abs(),
            ], axis=1).max(axis=1)
            
            atr = tr.ewm(alpha=1/period, adjust=False).mean()
            plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / atr)
            minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / atr)
            
            dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
            dx = dx.replace([np.inf, -np.inf], 0).fillna(0)
            
            adx = dx.ewm(alpha=1/period, adjust=False).mean().iloc[-1]
            return float(adx)
        except Exception:
            return None
    
    @staticmethod
    def extract_features(
        ohlcv_data: List[List[float]],
    ) -> Dict[str, float]:
        """
        Extract all features from OHLCV data.
        
        Returns dict with ~30 features.
        """
        if not ohlcv_data or len(ohlcv_data) < 200:
            return {}
        
        # Extract OHLCV
        timestamps = [c[0] for c in ohlcv_data]
        opens = [float(c[1]) for c in ohlcv_data]
        highs = [float(c[2]) for c in ohlcv_data]
        lows = [float(c[3]) for c in ohlcv_data]
        closes = [float(c[4]) for c in ohlcv_data]
        volumes = [float(c[5]) for c in ohlcv_data]
        
        current_price = closes[-1]
        current_volume = volumes[-1]
        
        features = {}
        
        # === TREND INDICATORS ===
        ema9 = MLFeatureExtractor._ema(closes, 9)
        ema21 = MLFeatureExtractor._ema(closes, 21)
        ema50 = MLFeatureExtractor._ema(closes, 50)
        ema200 = MLFeatureExtractor._ema(closes, 200)
        
        features['ema9'] = ema9 if ema9 else 0.0
        features['ema21'] = ema21 if ema21 else 0.0
        features['ema50'] = ema50 if ema50 else 0.0
        features['ema200'] = ema200 if ema200 else 0.0
        
        # Price position relative to EMAs
        features['price_vs_ema9'] = (current_price - ema9) / ema9 if ema9 else 0.0
        features['price_vs_ema21'] = (current_price - ema21) / ema21 if ema21 else 0.0
        features['price_vs_ema50'] = (current_price - ema50) / ema50 if ema50 else 0.0
        features['price_vs_ema200'] = (current_price - ema200) / ema200 if ema200 else 0.0
        
        # EMA alignment (trend strength)
        features['ema_alignment'] = 0.0
        if ema9 and ema21 and ema50:
            if ema9 > ema21 > ema50:
                features['ema_alignment'] = 1.0  # Strong uptrend
            elif ema9 < ema21 < ema50:
                features['ema_alignment'] = -1.0  # Strong downtrend
        
        # === MOMENTUM INDICATORS ===
        rsi = MLFeatureExtractor._rsi(closes, 14)
        features['rsi'] = rsi if rsi else 50.0
        features['rsi_oversold'] = 1.0 if rsi and rsi < 30 else 0.0
        features['rsi_overbought'] = 1.0 if rsi and rsi > 70 else 0.0
        
        # MACD
        ema12 = MLFeatureExtractor._ema(closes, 12)
        ema26 = MLFeatureExtractor._ema(closes, 26)
        if ema12 and ema26:
            macd = ema12 - ema26
            macd_signal = MLFeatureExtractor._ema([macd] * 20, 9) if macd else 0.0
            features['macd'] = macd
            features['macd_signal'] = macd_signal if macd_signal else 0.0
            features['macd_histogram'] = macd - (macd_signal if macd_signal else 0.0)
        else:
            features['macd'] = 0.0
            features['macd_signal'] = 0.0
            features['macd_histogram'] = 0.0
        
        # Rate of Change
        if len(closes) >= 10:
            roc_10 = (closes[-1] - closes[-10]) / closes[-10]
            features['roc_10'] = roc_10
        else:
            features['roc_10'] = 0.0
        
        # === VOLATILITY INDICATORS ===
        atr = MLFeatureExtractor._atr(highs, lows, closes, 14)
        features['atr'] = atr if atr else 0.0
        features['atr_pct'] = (atr / current_price) if atr else 0.0
        
        # Bollinger Bands
        if len(closes) >= 20:
            bb_mean = sum(closes[-20:]) / 20
            bb_std = (sum((x - bb_mean) ** 2 for x in closes[-20:]) / 20) ** 0.5
            bb_lower = bb_mean - 2 * bb_std
            bb_upper = bb_mean + 2 * bb_std
            bb_width = (bb_upper - bb_lower) / bb_mean
            
            features['bb_position'] = (current_price - bb_lower) / (bb_upper - bb_lower) if bb_upper > bb_lower else 0.5
            features['bb_width'] = bb_width
            features['bb_lower_breach'] = 1.0 if current_price < bb_lower else 0.0
            features['bb_upper_breach'] = 1.0 if current_price > bb_upper else 0.0
        else:
            features['bb_position'] = 0.5
            features['bb_width'] = 0.0
            features['bb_lower_breach'] = 0.0
            features['bb_upper_breach'] = 0.0
        
        # === VOLUME INDICATORS ===
        if len(volumes) >= 20:
            avg_volume = sum(volumes[-20:]) / 20
            features['volume_ratio'] = current_volume / avg_volume if avg_volume > 0 else 1.0
            
            # Volume trend
            recent_vol_avg = sum(volumes[-5:]) / 5
            older_vol_avg = sum(volumes[-20:-5]) / 15
            features['volume_trend'] = (recent_vol_avg - older_vol_avg) / older_vol_avg if older_vol_avg > 0 else 0.0
        else:
            features['volume_ratio'] = 1.0
            features['volume_trend'] = 0.0
        
        # === MARKET REGIME ===
        adx = MLFeatureExtractor._adx(highs, lows, closes, 14)
        features['adx'] = adx if adx else 0.0
        features['trending'] = 1.0 if adx and adx > 25 else 0.0
        features['choppy'] = 1.0 if adx and adx < 20 else 0.0
        
        # === PRICE PATTERNS ===
        # Recent price changes
        if len(closes) >= 48:  # 12 hours on 15min
            features['price_change_1h'] = (closes[-1] - closes[-5]) / closes[-5]
            features['price_change_4h'] = (closes[-1] - closes[-17]) / closes[-17]
            features['price_change_12h'] = (closes[-1] - closes[-49]) / closes[-49]
        else:
            features['price_change_1h'] = 0.0
            features['price_change_4h'] = 0.0
            features['price_change_12h'] = 0.0
        
        # Higher highs / lower lows
        if len(highs) >= 20:
            recent_high = max(highs[-10:])
            prev_high = max(highs[-20:-10])
            features['higher_high'] = 1.0 if recent_high > prev_high else 0.0
            
            recent_low = min(lows[-10:])
            prev_low = min(lows[-20:-10])
            features['lower_low'] = 1.0 if recent_low < prev_low else 0.0
        else:
            features['higher_high'] = 0.0
            features['lower_low'] = 0.0
        
        return features
    
    @staticmethod
    def get_feature_names() -> List[str]:
        """Get list of all feature names"""
        return [
            # Trend
            'ema9', 'ema21', 'ema50', 'ema200',
            'price_vs_ema9', 'price_vs_ema21', 'price_vs_ema50', 'price_vs_ema200',
            'ema_alignment',
            
            # Momentum
            'rsi', 'rsi_oversold', 'rsi_overbought',
            'macd', 'macd_signal', 'macd_histogram',
            'roc_10',
            
            # Volatility
            'atr', 'atr_pct',
            'bb_position', 'bb_width', 'bb_lower_breach', 'bb_upper_breach',
            
            # Volume
            'volume_ratio', 'volume_trend',
            
            # Regime
            'adx', 'trending', 'choppy',
            
            # Patterns
            'price_change_1h', 'price_change_4h', 'price_change_12h',
            'higher_high', 'lower_low',
        ]
