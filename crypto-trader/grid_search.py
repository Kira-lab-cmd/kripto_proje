#!/usr/bin/env python3
"""
Parameter Grid Search - Day 2 Morning

Find optimal parameters for:
- Breakout percentile
- ATR stop/take multipliers
- Volume ratio threshold
- Min gates required

Goal: Increase win rate from 36.4% to 50%+
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Parameter grid
PARAM_GRID = {
    "breakout_percentile": [0.85, 0.90, 0.95],  # 85th, 90th, 95th percentile
    "min_volume_ratio": [0.5, 0.75, 1.0],       # Volume threshold
    "atr_sl_mult": [1.5, 2.0, 2.5],             # Stop loss
    "atr_tp_mult": [3.0, 3.5, 4.0],             # Take profit
    "min_gates_required": [2, 3],               # 2/4 or 3/4 gates
}

TOTAL_COMBINATIONS = (
    len(PARAM_GRID["breakout_percentile"]) *
    len(PARAM_GRID["min_volume_ratio"]) *
    len(PARAM_GRID["atr_sl_mult"]) *
    len(PARAM_GRID["atr_tp_mult"]) *
    len(PARAM_GRID["min_gates_required"])
)

print("=" * 80)
print("PARAMETER GRID SEARCH - DAY 2")
print("=" * 80)
print()
print(f"Total Combinations: {TOTAL_COMBINATIONS}")
print()
print("Parameter Ranges:")
for param, values in PARAM_GRID.items():
    print(f"  {param:<25} {values}")
print()
print("Estimated Time: ~30-45 minutes")
print()

# Ask for confirmation
response = input("Start grid search? (y/n): ")
if response.lower() != 'y':
    print("Aborted.")
    sys.exit(0)

print("\n🚀 Starting grid search...\n")

results = []
best_result = None
best_win_rate = 0.0

import itertools

for i, (bp, vr, sl, tp, mg) in enumerate(itertools.product(
    PARAM_GRID["breakout_percentile"],
    PARAM_GRID["min_volume_ratio"],
    PARAM_GRID["atr_sl_mult"],
    PARAM_GRID["atr_tp_mult"],
    PARAM_GRID["min_gates_required"]
), 1):
    
    print(f"\n[{i}/{TOTAL_COMBINATIONS}] Testing:")
    print(f"  Breakout Percentile: {bp}")
    print(f"  Volume Ratio: {vr}")
    print(f"  SL Mult: {sl}")
    print(f"  TP Mult: {tp}")
    print(f"  Min Gates: {mg}")
    
    # Set environment variables
    import os
    os.environ["BREAKOUT_PERCENTILE"] = str(bp)
    os.environ["MIN_VOLUME_RATIO"] = str(vr)
    os.environ["ATR_SL_MULT"] = str(sl)
    os.environ["ATR_TP_MULT"] = str(tp)
    os.environ["MIN_GATES_REQUIRED"] = str(mg)
    
    # Run backtest
    cmd = [
        sys.executable, "-m", "backend.walkforward",
        "--start", "2025-12-01",
        "--end", "2026-02-22",
        "--train-days", "40",
        "--test-days", "14",
        "--step-days", "14",
        "--execution-model", "realistic",
        "--buy-th-grid", "2.5",
        "--sell-th-grid", "-2.5",
        "--atr-sl-grid", str(sl),
        "--atr-tp-grid", str(tp),
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 min timeout
        )
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            agg = data["aggregated"]
            
            win_rate = agg["avg_test_win_rate"]
            trade_count = agg["avg_test_trade_count"]
            pnl = agg["total_test_net_pnl"]
            
            print(f"  ✅ Win Rate: {win_rate*100:.1f}% | Trades: {trade_count:.0f} | PnL: ${pnl:.2f}")
            
            result_entry = {
                "params": {
                    "breakout_percentile": bp,
                    "min_volume_ratio": vr,
                    "atr_sl_mult": sl,
                    "atr_tp_mult": tp,
                    "min_gates_required": mg,
                },
                "metrics": {
                    "win_rate": win_rate,
                    "trade_count": trade_count,
                    "pnl": pnl,
                }
            }
            results.append(result_entry)
            
            # Track best
            if win_rate > best_win_rate:
                best_win_rate = win_rate
                best_result = result_entry
                print(f"  🌟 NEW BEST! Win Rate: {win_rate*100:.1f}%")
        
        else:
            print(f"  ❌ Failed: {result.stderr[:100]}")
    
    except subprocess.TimeoutExpired:
        print(f"  ⏱️ Timeout (>5min)")
    except Exception as e:
        print(f"  ❌ Error: {e}")

print("\n" + "=" * 80)
print("GRID SEARCH COMPLETE!")
print("=" * 80)

if best_result:
    print("\n🏆 BEST PARAMETERS:")
    print()
    for param, value in best_result["params"].items():
        print(f"  {param:<25} {value}")
    print()
    print("📊 BEST METRICS:")
    metrics = best_result["metrics"]
    print(f"  Win Rate:     {metrics['win_rate']*100:.1f}%")
    print(f"  Trade Count:  {metrics['trade_count']:.0f}")
    print(f"  Total PnL:    ${metrics['pnl']:.2f}")
    print()
    
    # Save results
    output_file = Path("grid_search_results.json")
    with open(output_file, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "total_tested": len(results),
            "best_params": best_result["params"],
            "best_metrics": best_result["metrics"],
            "all_results": results
        }, f, indent=2)
    
    print(f"📄 Results saved to: {output_file}")
    
    # Generate .env updates
    print("\n📝 UPDATE YOUR .env FILE:")
    print()
    print("# Optimized Parameters (Grid Search Results)")
    print(f"BREAKOUT_PERCENTILE={best_result['params']['breakout_percentile']}")
    print(f"MIN_VOLUME_RATIO={best_result['params']['min_volume_ratio']}")
    print(f"ATR_SL_MULT={best_result['params']['atr_sl_mult']}")
    print(f"ATR_TP_MULT={best_result['params']['atr_tp_mult']}")
    print(f"MIN_GATES_REQUIRED={best_result['params']['min_gates_required']}")
    
else:
    print("\n❌ No successful results!")

print("\n✅ Grid search complete!")
