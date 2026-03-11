#!/usr/bin/env python3
"""
Strategy V1 vs V2 Comparison Test

Quick test to compare old (broken) vs new (fixed) strategy logic.
This will show us if the fixes actually improve signal generation.
"""

import json
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

from backend.strategy import TradingStrategy as StrategyV1
from backend.strategy_v2 import TradingStrategyV2 as StrategyV2


def generate_sample_ohlcv(trend="up", volatility="normal"):
    """
    Generate synthetic OHLCV data for testing.
    
    Args:
        trend: "up", "down", "sideways"
        volatility: "low", "normal", "high"
    """
    import random
    
    base_price = 50000.0
    ohlcv = []
    
    # Generate 250 candles (enough for EMA200 + lookback)
    for i in range(250):
        if trend == "up":
            base_price *= 1.002  # +0.2% per candle
        elif trend == "down":
            base_price *= 0.998  # -0.2% per candle
        # else sideways - no change
        
        if volatility == "low":
            vol_factor = 0.003
        elif volatility == "high":
            vol_factor = 0.015
        else:  # normal
            vol_factor = 0.007
        
        noise = random.uniform(-vol_factor, vol_factor)
        close = base_price * (1 + noise)
        
        high = close * (1 + abs(random.uniform(0, vol_factor/2)))
        low = close * (1 - abs(random.uniform(0, vol_factor/2)))
        open_ = (high + low) / 2
        
        volume = random.uniform(1000, 2000)
        
        ohlcv.append([
            i * 900000,  # timestamp (15min intervals)
            open_,
            high,
            low,
            close,
            volume
        ])
    
    return ohlcv


def test_strategy_comparison():
    """Compare V1 vs V2 on various market conditions"""
    
    print("=" * 80)
    print("STRATEGY V1 vs V2 COMPARISON TEST")
    print("=" * 80)
    print()
    
    scenarios = [
        ("Uptrend + Normal Vol", "up", "normal"),
        ("Uptrend + High Vol", "up", "high"),
        ("Sideways + Normal Vol", "sideways", "normal"),
        ("Downtrend + Normal Vol", "down", "normal"),
    ]
    
    v1_strategy = StrategyV1()
    v2_strategy = StrategyV2()
    
    results = {
        "v1": {"buy": 0, "hold": 0, "total": 0},
        "v2": {"buy": 0, "hold": 0, "total": 0},
        "scenarios": []
    }
    
    for scenario_name, trend, vol in scenarios:
        print(f"\n📊 Scenario: {scenario_name}")
        print("-" * 80)
        
        ohlcv = generate_sample_ohlcv(trend=trend, volatility=vol)
        
        # Test V1
        v1_signal = v1_strategy.get_signal(
            ohlcv, 
            sentiment_score=0.0,
            symbol="BTC/USDT",
            trend_dir_1h="UP" if trend == "up" else "NEUTRAL"
        )
        
        # Test V2
        v2_signal = v2_strategy.get_signal(
            ohlcv,
            sentiment_score=0.0,
            symbol="BTC/USDT",
            trend_dir_1h="UP" if trend == "up" else "NEUTRAL"
        )
        
        # Count signals
        results["v1"]["total"] += 1
        results["v2"]["total"] += 1
        
        if v1_signal["signal"] == "BUY":
            results["v1"]["buy"] += 1
        else:
            results["v1"]["hold"] += 1
        
        if v2_signal["signal"] == "BUY":
            results["v2"]["buy"] += 1
        else:
            results["v2"]["hold"] += 1
        
        # Print comparison
        print(f"V1 Signal: {v1_signal['signal']:<4} | Score: {v1_signal['score']:.1f} | "
              f"Reason: {v1_signal['reason'][:60]}...")
        print(f"V2 Signal: {v2_signal['signal']:<4} | Score: {v2_signal['score']:.1f} | "
              f"Reason: {v2_signal['reason'][:60]}...")
        
        # Detailed gate comparison
        print("\nGate Status Comparison:")
        print(f"{'Gate':<20} | {'V1':<8} | {'V2':<8}")
        print("-" * 40)
        
        for gate in ["ema_uptrend", "bias_not_down", "atr_ok", "volume_ok", "breakout_ok"]:
            v1_status = "✅ PASS" if v1_signal["gate_status"].get(gate, False) else "❌ FAIL"
            v2_status = "✅ PASS" if v2_signal["gate_status"].get(gate, False) else "❌ FAIL"
            print(f"{gate:<20} | {v1_status:<8} | {v2_status:<8}")
        
        scenario_result = {
            "name": scenario_name,
            "v1_signal": v1_signal["signal"],
            "v2_signal": v2_signal["signal"],
            "v1_score": v1_signal["score"],
            "v2_score": v2_signal["score"],
        }
        results["scenarios"].append(scenario_result)
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    v1_buy_rate = (results["v1"]["buy"] / results["v1"]["total"]) * 100
    v2_buy_rate = (results["v2"]["buy"] / results["v2"]["total"]) * 100
    
    print(f"\nV1 Strategy (BROKEN):")
    print(f"  BUY signals:  {results['v1']['buy']}/{results['v1']['total']} ({v1_buy_rate:.1f}%)")
    print(f"  HOLD signals: {results['v1']['hold']}/{results['v1']['total']} ({100-v1_buy_rate:.1f}%)")
    
    print(f"\nV2 Strategy (FIXED):")
    print(f"  BUY signals:  {results['v2']['buy']}/{results['v2']['total']} ({v2_buy_rate:.1f}%)")
    print(f"  HOLD signals: {results['v2']['hold']}/{results['v2']['total']} ({100-v2_buy_rate:.1f}%)")
    
    improvement = v2_buy_rate - v1_buy_rate
    print(f"\n🎯 Signal Generation Improvement: {improvement:+.1f}%")
    
    if improvement > 0:
        print("✅ V2 generates MORE signals (good - reduces blockage!)")
    else:
        print("⚠️ V2 generates FEWER signals (needs investigation)")
    
    # Save results
    output_file = Path(__file__).parent / "strategy_comparison.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\n📄 Full results saved to: {output_file}")
    
    return results


if __name__ == "__main__":
    test_strategy_comparison()
