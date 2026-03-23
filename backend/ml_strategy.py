"""
ML-Powered Trading Strategy

Uses trained Random Forest model to make trading decisions.

Expected win rate: 55-65% (if patterns exist in data)
"""

from __future__ import annotations

import os
import pickle
from typing import Any, Dict

import pandas as pd

from .ml_features import MLFeatureExtractor
from .indicators import Indicators
from .utils_symbols import normalize_symbol

GATE_STATUS_KEYS = (
    "ml_confidence",
    "features_valid",
    "volume_ok",
    "atr_ok",
)

ALL_GATE_STATUS_KEYS = (
    "ml_confidence",
    "features_valid",
    "volume_ok",
    "atr_ok",
)


class TradingStrategy:
    """
    ML-Powered Trading Strategy
    
    Uses Random Forest to predict trade success.
    Only trades when ML confidence > threshold.
    """

    def __init__(self) -> None:
        # ML settings
        self.ml_model_path = os.getenv("ML_MODEL_PATH", "ml_model.pkl")
        self.ml_confidence_threshold = float(os.getenv("ML_CONFIDENCE_THRESHOLD", "0.70"))
        
        # Risk management
        self.atr_sl_mult = float(os.getenv("ATR_SL_MULT", "2.0"))
        self.atr_tp_mult = float(os.getenv("ATR_TP_MULT", "3.0"))
        
        # Filters
        self.min_atr_pct = float(os.getenv("MIN_ATR_PCT", "0.005"))
        self.max_atr_pct = float(os.getenv("MAX_ATR_PCT", "0.025"))
        self.min_volume_ratio = float(os.getenv("MIN_VOLUME_RATIO", "0.75"))
        
        # Load ML model
        self.ml_model = None
        self.feature_extractor = MLFeatureExtractor()
        self.feature_names = self.feature_extractor.get_feature_names()
        
        self._load_ml_model()

    def _load_ml_model(self):
        """Load trained ML model"""
        try:
            with open(self.ml_model_path, 'rb') as f:
                self.ml_model = pickle.load(f)
            print(f"✅ ML model loaded from {self.ml_model_path}")
        except FileNotFoundError:
            print(f"⚠️  ML model not found at {self.ml_model_path}")
            print(f"⚠️  Train model first using: python -m backend.train_ml_model")
            self.ml_model = None
        except Exception as e:
            print(f"❌ Error loading ML model: {e}")
            self.ml_model = None

    def get_config_snapshot(self) -> Dict[str, Any]:
        return {
            "version": "ml_powered_v1",
            "ml_model_path": str(self.ml_model_path),
            "ml_confidence_threshold": float(self.ml_confidence_threshold),
            "atr_sl_mult": float(self.atr_sl_mult),
            "atr_tp_mult": float(self.atr_tp_mult),
            "min_atr_pct": float(self.min_atr_pct),
            "max_atr_pct": float(self.max_atr_pct),
            "min_volume_ratio": float(self.min_volume_ratio),
            "model_loaded": self.ml_model is not None,
        }

    def _hold(
        self,
        *,
        reason: str,
        current_price: float | None,
        **kwargs
    ) -> Dict[str, Any]:
        return {
            "signal": "HOLD",
            "score": 0.0,
            "current_price": float(current_price) if current_price is not None else None,
            "stop_loss": None,
            "take_profit": None,
            "reason": reason,
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
        
        # Check if ML model is loaded
        if self.ml_model is None:
            return self._hold(
                reason="ML model not loaded",
                current_price=None,
                gate_status={name: False for name in ALL_GATE_STATUS_KEYS},
                hold_fail_reasons=["no_model"],
            )
        
        # Check data sufficiency
        min_required = 250
        if not ohlcv_data or len(ohlcv_data) < min_required:
            return self._hold(
                reason="Insufficient data for ML features",
                current_price=None,
                gate_status={name: False for name in ALL_GATE_STATUS_KEYS},
                hold_fail_reasons=["insufficient_data"],
            )

        # Extract OHLCV
        closes = [float(c[4]) for c in ohlcv_data]
        highs = [float(c[2]) for c in ohlcv_data]
        lows = [float(c[3]) for c in ohlcv_data]
        volumes = [float(c[5]) for c in ohlcv_data]

        current_price = float(closes[-1])
        
        # Extract ML features
        features = self.feature_extractor.extract_features(ohlcv_data)
        
        if not features:
            return self._hold(
                reason="Failed to extract features",
                current_price=current_price,
                gate_status={name: False for name in ALL_GATE_STATUS_KEYS},
                hold_fail_reasons=["feature_extraction_failed"],
            )
        
        # Convert features to DataFrame
        feature_vector = pd.DataFrame([features])
        
        # Ensure all features present
        for feature_name in self.feature_names:
            if feature_name not in feature_vector.columns:
                feature_vector[feature_name] = 0.0
        
        # Reorder to match training
        feature_vector = feature_vector[self.feature_names]
        
        # Get ML prediction
        try:
            prediction_proba = self.ml_model.predict_proba(feature_vector)[0]
            win_probability = float(prediction_proba[1])  # Probability of WIN
            prediction = "WIN" if win_probability >= self.ml_confidence_threshold else "LOSS"
        except Exception as e:
            return self._hold(
                reason=f"ML prediction failed: {e}",
                current_price=current_price,
                gate_status={name: False for name in ALL_GATE_STATUS_KEYS},
                hold_fail_reasons=["prediction_failed"],
            )
        
        # Calculate basic indicators for risk management
        atr = Indicators.calculate_atr(highs, lows, closes)
        atr_pct = (float(atr) / current_price) if atr and current_price > 0 else None
        
        # Volume check
        avg_volume = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else volumes[-1]
        vol_ratio = (volumes[-1] / avg_volume) if avg_volume > 0 else None
        
        # Gate checks
        ml_confident = win_probability >= self.ml_confidence_threshold
        features_valid = True  # Features extracted successfully
        volume_ok = vol_ratio is not None and vol_ratio >= self.min_volume_ratio
        atr_ok = atr_pct is not None and self.min_atr_pct <= atr_pct <= self.max_atr_pct
        
        gate_status = {
            "ml_confidence": ml_confident,
            "features_valid": features_valid,
            "volume_ok": volume_ok,
            "atr_ok": atr_ok,
        }
        
        hold_fail_reasons = [name for name in GATE_STATUS_KEYS if not gate_status.get(name, False)]
        
        # Reasons
        reasons = []
        reasons.append(f"ML prediction: {prediction} ({win_probability:.1%} confidence)")
        
        if not ml_confident:
            reasons.append(f"Low confidence (need {self.ml_confidence_threshold:.0%})")
        
        if volume_ok:
            reasons.append(f"Volume OK ({vol_ratio:.2f}x)")
        else:
            reasons.append("Volume weak")
        
        if atr_ok:
            reasons.append(f"ATR OK ({atr_pct:.2%})")
        else:
            reasons.append("ATR out of range")
        
        # Entry decision
        should_buy = ml_confident and volume_ok and atr_ok
        
        if not should_buy:
            sym = normalize_symbol(symbol or "")
            if sym:
                reasons.append(f"SYM={sym}")
            
            return self._hold(
                reason=", ".join(reasons),
                current_price=current_price,
                ml_prediction=prediction,
                ml_confidence=win_probability,
                atr=atr,
                vol_ratio=vol_ratio,
                gate_status=gate_status,
                hold_fail_reasons=hold_fail_reasons,
            )
        
        # BUY Signal
        stop_loss = current_price - (self.atr_sl_mult * float(atr))
        take_profit = current_price + (self.atr_tp_mult * float(atr))
        
        sym = normalize_symbol(symbol or "")
        if sym:
            reasons.append(f"SYM={sym}")
        
        return {
            "signal": "BUY",
            "score": win_probability * 10.0,  # Scale to 0-10
            "current_price": float(current_price),
            "stop_loss": round(float(stop_loss), 8),
            "take_profit": round(float(take_profit), 8),
            "reason": ", ".join(reasons),
            "ml_prediction": prediction,
            "ml_confidence": float(win_probability),
            "atr": float(atr) if atr is not None else None,
            "vol_ratio": float(vol_ratio) if vol_ratio is not None else None,
            "gate_status": gate_status,
            "hold_fail_reasons": [],
            "regime": "ML_DRIVEN",
            "regime_conf": float(win_probability),
        }
