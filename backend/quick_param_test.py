#!/usr/bin/env python3
"""
Quick Parameter Test - Day 2 Morning

Test a few key parameter combinations quickly.
Full grid search has 54 combinations (too slow).
This tests 6 promising combinations (~10-15 min).
"""

import json
import subprocess
import sys
from pathlib import Path

# Promising combinations based on theory
QUICK_TESTS = [
    # (breakout_pct, vol_ratio, sl_mult, tp_mult, min_gates, name)
    (0.90, 0.75, 2.0, 3.0, 3, "Baseline (Current V2)"),
    (0.85, 0.75, 2.0, 3.5, 3, "Lower breakout + Higher TP"),
    (0.90, 1.0, 1.5, 3.5, 3, "Stricter vol + Tighter SL"),
    (0.95, 0.5, 2.0, 4.0, 2, "High breakout + Relaxed gates"),
    (0.85, 1.0, 2.0, 3.0, 3, "Low breakout + High vol"),
    (0.90, 0.75, 2.5, 3.5, 2, "Wider SL + Relaxed gates"),
]

print("=" * 80)
print("QUICK PARAMETER TEST - DAY 2")
print("=" * 80)
print()
print(f"Testing {len(QUICK_TESTS)} combinations")
print("Estimated time: 10-15 minutes")
print()

results = []
best_win_rate = 0.0
best_result = None

for i, (bp, vr, sl, tp, mg, name) in enumerate(QUICK_TESTS, 1):
    print(f"\n[{i}/{len(QUICK_TESTS)}] {name}")
    print(f"  Params: BP={bp}, Vol={vr}, SL={sl}x, TP={tp}x, Gates={mg}/4")
    
    # Set env vars
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
        print("  Running backtest...")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=180  # 3 min timeout
        )
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            agg = data["aggregated"]
            
            win_rate = agg["avg_test_win_rate"]
            trade_count = agg["avg_test_trade_count"]
            pnl = agg["total_test_net_pnl"]
            
            print(f"  ✅ Win Rate: {win_rate*100:.1f}% | Trades: {trade_count:.0f} | PnL: ${pnl:.2f}")
            
            result_entry = {
                "name": name,
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
            
            if win_rate > best_win_rate:
                best_win_rate = win_rate
                best_result = result_entry
                print(f"  🌟 NEW BEST!")
        
        else:
            print(f"  ❌ Failed")
    
    except subprocess.TimeoutExpired:
        print(f"  ⏱️ Timeout")
    except Exception as e:
        print(f"  ❌ Error: {e}")

print("\n" + "=" * 80)
print("QUICK TEST COMPLETE!")
print("=" * 80)

if results:
    print("\n📊 ALL RESULTS (sorted by win rate):")
    print()
    sorted_results = sorted(results, key=lambda x: x["metrics"]["win_rate"], reverse=True)
    
    for i, r in enumerate(sorted_results, 1):
        metrics = r["metrics"]
        print(f"{i}. {r['name']}")
        print(f"   Win Rate: {metrics['win_rate']*100:.1f}% | "
              f"Trades: {metrics['trade_count']:.0f} | "
              f"PnL: ${metrics['pnl']:.2f}")
    
    print("\n🏆 BEST PARAMETERS:")
    print()
    print(f"Name: {best_result['name']}")
    print()
    for param, value in best_result["params"].items():
        print(f"  {param:<25} {value}")
    print()
    metrics = best_result["metrics"]
    print(f"Win Rate:     {metrics['win_rate']*100:.1f}%")
    print(f"Trade Count:  {metrics['trade_count']:.0f}")
    print(f"Total PnL:    ${metrics['pnl']:.2f}")
    
    # Save
    output = Path("quick_test_results.json")
    with open(output, "w") as f:
        json.dump({
            "best_params": best_result["params"],
            "best_metrics": best_result["metrics"],
            "all_results": sorted_results
        }, f, indent=2)
    
    print(f"\n📄 Saved to: {output}")
    
    # .env suggestion
    print("\n📝 SUGGESTED .ENV UPDATES:")
    print()
    for param, value in best_result["params"].items():
        print(f"{param.upper()}={value}")

else:
    print("\n❌ No successful results!")
