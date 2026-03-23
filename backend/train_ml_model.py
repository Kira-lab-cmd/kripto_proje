"""
Simplified ML Model Training Script

Uses existing project infrastructure (ResearchStore, ReplayEngine)

Usage:
    python -m backend.train_ml_model --start 2025-12-01 --end 2026-02-22
"""

import os
import sys
import argparse
import pickle
from datetime import datetime, timezone
from typing import List, Dict, Tuple

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import classification_report, confusion_matrix

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.ml_features import MLFeatureExtractor
from backend.research_store import ResearchStore
from backend.replay_engine import _parse_date


class SimplifiedMLTrainer:
    """Simplified ML trainer using ResearchStore"""
    
    def __init__(self):
        self.feature_extractor = MLFeatureExtractor()
        self.feature_names = self.feature_extractor.get_feature_names()
        
        # Get research DB path from env or use default
        research_db_path = os.getenv("RESEARCH_DB_PATH", "research.db").strip() or "research.db"
        self.store = ResearchStore(db_path=research_db_path)
        
    def collect_training_data(
        self,
        start_date: str,
        end_date: str,
        symbols: List[str] = None
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Collect training data from ResearchStore.
        
        Simpler approach: Load OHLCV, generate features, simulate trades.
        """
        if symbols is None:
            symbols = ["BTC/USDT", "ETH/USDT"]
        
        print(f"\n{'='*80}")
        print(f"COLLECTING TRAINING DATA")
        print(f"{'='*80}")
        print(f"Period: {start_date} to {end_date}")
        print(f"Symbols: {symbols}")
        print(f"{'='*80}\n")
        
        start_dt = _parse_date(start_date)
        end_dt = _parse_date(end_date)
        
        all_features = []
        all_labels = []
        
        for symbol in symbols:
            print(f"\n📊 Processing {symbol}...")
            
            # Load OHLCV from ResearchStore
            try:
                rows = self.store.load(
                    symbol=symbol,
                    timeframe="15m",
                    since_ms=int(start_dt.timestamp() * 1000),
                )
                
                if not rows:
                    print(f"  ⚠️  No data found for {symbol}")
                    continue
                
                # Filter to end date
                rows = [r for r in rows if r.ts_ms <= int(end_dt.timestamp() * 1000)]
                
                # Convert to OHLCV format
                ohlcv = [[r.ts_ms, r.open, r.high, r.low, r.close, r.volume] for r in rows]
                
                print(f"  ✅ Loaded {len(ohlcv)} candles")
                
            except Exception as e:
                print(f"  ❌ Error loading data for {symbol}: {e}")
                continue
            
            if len(ohlcv) < 300:
                print(f"  ⚠️  Insufficient data ({len(ohlcv)} candles)")
                continue
            
            # Generate training samples
            window_size = 250
            sample_count = 0
            
            for i in range(window_size, len(ohlcv) - 50, 10):  # Sample every 10 candles
                window = ohlcv[max(0, i-window_size):i+1]
                
                if len(window) < window_size:
                    continue
                
                # Extract features
                try:
                    features = self.feature_extractor.extract_features(window)
                    
                    if not features:
                        continue
                    
                    # Simulate simple trade outcome
                    # Entry at current close
                    entry_price = float(window[-1][4])
                    
                    # Simple SL/TP (2% stop, 3% target)
                    stop_loss = entry_price * 0.98
                    take_profit = entry_price * 1.03
                    
                    # Look ahead 50 candles
                    future = ohlcv[i+1:i+51]
                    
                    label = 0  # Default: LOSS
                    
                    for candle in future:
                        low = float(candle[3])
                        high = float(candle[2])
                        
                        # Check SL
                        if low <= stop_loss:
                            label = 0  # LOSS
                            break
                        
                        # Check TP
                        if high >= take_profit:
                            label = 1  # WIN
                            break
                    
                    all_features.append(features)
                    all_labels.append(label)
                    sample_count += 1
                    
                    if sample_count % 50 == 0:
                        print(f"  Samples: {sample_count}...", end='\r')
                
                except Exception as e:
                    continue
            
            print(f"  ✅ Collected {sample_count} samples from {symbol}           ")
        
        print(f"\n{'='*80}")
        print(f"TOTAL SAMPLES COLLECTED: {len(all_features)}")
        print(f"{'='*80}\n")
        
        if len(all_features) < 100:
            print(f"❌ ERROR: Only {len(all_features)} samples collected!")
            print(f"   Need at least 100 samples for training")
            return None, None
        
        # Convert to DataFrame
        X = pd.DataFrame(all_features)
        y = pd.Series(all_labels)
        
        # Ensure all features present
        for feature in self.feature_names:
            if feature not in X.columns:
                X[feature] = 0.0
        
        # Reorder columns
        X = X[self.feature_names]
        
        return X, y
    
    def train_model(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        test_size: float = 0.2
    ) -> RandomForestClassifier:
        """Train Random Forest model"""
        
        print(f"\n{'='*80}")
        print("TRAINING MACHINE LEARNING MODEL")
        print(f"{'='*80}\n")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42, stratify=y
        )
        
        print(f"📊 Dataset Split:")
        print(f"  Total samples:    {len(X)}")
        print(f"  Training samples: {len(X_train)}")
        print(f"  Test samples:     {len(X_test)}")
        print(f"  Overall win rate: {y.mean()*100:.1f}%")
        print()
        
        # Train Random Forest
        print("🌲 Training Random Forest...")
        
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=20,
            min_samples_leaf=10,
            random_state=42,
            n_jobs=-1,
            verbose=0
        )
        
        model.fit(X_train, y_train)
        print("  ✅ Model trained!")
        print()
        
        # Cross-validation
        print("🔄 Cross-validation (5-fold)...")
        cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='accuracy')
        print(f"  CV Accuracy: {cv_scores.mean():.3f} (+/- {cv_scores.std()*2:.3f})")
        
        if cv_scores.mean() >= 0.60:
            print("  ✅ GOOD! Accuracy >60%")
        elif cv_scores.mean() >= 0.55:
            print("  ⚠️  MARGINAL. Accuracy 55-60%")
        else:
            print("  ❌ WEAK. Accuracy <55%")
        print()
        
        # Test set evaluation
        y_pred = model.predict(X_test)
        y_pred_proba = model.predict_proba(X_test)[:, 1]
        
        print("📈 Test Set Performance:")
        print(classification_report(y_test, y_pred, target_names=['LOSS', 'WIN']))
        
        print("📊 Confusion Matrix:")
        cm = confusion_matrix(y_test, y_pred)
        print(f"  True Neg:  {cm[0,0]:>4}  |  False Pos: {cm[0,1]:>4}")
        print(f"  False Neg: {cm[1,0]:>4}  |  True Pos:  {cm[1,1]:>4}")
        print()
        
        # Feature importance
        print("🔝 Top 10 Most Important Features:")
        feature_importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        for idx, row in feature_importance.head(10).iterrows():
            print(f"  {row['feature']:<25} {row['importance']:.4f}")
        print()
        
        # Confidence calibration
        print("🎯 Prediction Confidence Analysis:")
        confidence_thresholds = [0.5, 0.6, 0.7, 0.8]
        
        for threshold in confidence_thresholds:
            high_conf_mask = y_pred_proba >= threshold
            if high_conf_mask.sum() > 0:
                high_conf_accuracy = (y_test[high_conf_mask] == y_pred[high_conf_mask]).mean()
                status = "✅" if high_conf_accuracy >= 0.65 else "⚠️" if high_conf_accuracy >= 0.60 else "❌"
                print(f"  {status} Confidence ≥{threshold:.1f}: {high_conf_accuracy:.1%} accuracy ({high_conf_mask.sum()} samples)")
        print()
        
        return model
    
    def save_model(self, model: RandomForestClassifier, path: str = "ml_model.pkl"):
        """Save trained model"""
        with open(path, 'wb') as f:
            pickle.dump(model, f)
        print(f"💾 Model saved to: {path}\n")


def main():
    parser = argparse.ArgumentParser(description="Train ML model for trading")
    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default="ml_model.pkl", help="Output model path")
    
    args = parser.parse_args()
    
    print(f"\n{'='*80}")
    print("ML MODEL TRAINING PIPELINE")
    print(f"{'='*80}")
    print(f"Start Date:  {args.start}")
    print(f"End Date:    {args.end}")
    print(f"Output Path: {args.output}")
    print(f"{'='*80}")
    
    # Train model
    trainer = SimplifiedMLTrainer()
    
    # Collect training data
    X, y = trainer.collect_training_data(args.start, args.end)
    
    if X is None or len(X) < 100:
        print("\n❌ ERROR: Insufficient training data!")
        print(f"   Need at least 100 samples, got: {len(X) if X is not None else 0}")
        return 1
    
    # Train model
    model = trainer.train_model(X, y)
    
    # Save model
    trainer.save_model(model, args.output)
    
    print(f"{'='*80}")
    print("✅ TRAINING COMPLETE!")
    print(f"{'='*80}\n")
    print(f"Model saved to: {args.output}\n")
    print("📝 Next Steps:")
    print("  1. Review model performance above")
    print("  2. Check CV Accuracy (target: >60%)")
    print("  3. Check Confidence ≥0.7 accuracy (target: >65%)")
    print("  4. If good → Run backtest!")
    print("  5. If backtest win rate >50% → Paper trading!")
    print()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
