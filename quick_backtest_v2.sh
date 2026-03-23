#!/bin/bash
# Quick Backtest - Strategy V2
# Single fold test to validate improvements

echo "🚀 STRATEGY V2 - QUICK BACKTEST"
echo "================================"
echo ""
echo "Config:"
echo "  Period: 2026-02-01 to 2026-03-07 (35 days)"
echo "  Train: 20 days, Test: 10 days"
echo "  Symbols: BTC/USDT, ETH/USDT"
echo "  Execution: Realistic (fees + slippage)"
echo ""
echo "Starting..."
echo ""

cd /home/claude/proje

python3 -m backend.walkforward \
    --start 2026-02-01 \
    --end 2026-03-07 \
    --train-days 20 \
    --test-days 10 \
    --step-days 10 \
    --execution-model realistic \
    --buy-th-grid "2.5" \
    --sell-th-grid "-2.5" \
    --atr-sl-grid "2.0" \
    --atr-tp-grid "3.0" \
    > results_v2_quick.json

echo ""
echo "✅ Backtest completed!"
echo "📄 Results: results_v2_quick.json"
echo ""
echo "Quick Analysis:"
python3 -c "
import json
with open('results_v2_quick.json') as f:
    data = json.load(f)
    
agg = data['aggregated']
print(f\"  Total Test PnL: \${agg['total_test_net_pnl']:.2f}\")
print(f\"  Avg Win Rate: {agg['avg_test_win_rate']*100:.1f}%\")
print(f\"  Avg Trades/Fold: {agg['avg_test_trade_count']:.0f}\")
print(f\"  Max DD: {agg['avg_test_max_dd_pct']*100:.2f}%\")
print()

if agg['avg_test_win_rate'] > 0.5 and agg['total_test_net_pnl'] > 0:
    print('🎉 SUCCESS! V2 is working!')
elif agg['avg_test_win_rate'] > 0.45:
    print('⚠️  PROMISING - needs parameter tuning')
else:
    print('❌ NEEDS MORE WORK')
"
