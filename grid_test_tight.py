"""
GRID STRATEGY TEST - TIGHTER GRIDS
Uses 100 grids instead of 20 for more frequent crossings
"""

import os
import sys
import ccxt

print("\n" + "="*80)
print("GRID STRATEGY TEST - TIGHTER GRIDS (100 grids)")
print("="*80)

# Get current price
exchange = ccxt.binance()
ticker = exchange.fetch_ticker("BTC/USDT")
current_btc = ticker['last']

lower_price = int(current_btc * 0.94)  # -6%
upper_price = int(current_btc * 1.06)  # +6%

# USE 100 GRIDS INSTEAD OF 20! (5x tighter)
os.environ["GRID_LOWER_PRICE"] = str(lower_price)
os.environ["GRID_UPPER_PRICE"] = str(upper_price)
os.environ["GRID_COUNT"] = "100"  # ⭐ 100 grids!
os.environ["GRID_CAPITAL_PER_LEVEL"] = "10.0"  # $10 per grid (smaller capital)

# Also lower ATR requirement
os.environ["MIN_ATR_PCT"] = "0.001"  # 0.1% instead of 0.3%

from backend.grid_strategy import TradingStrategy

strategy = TradingStrategy()

print(f"\n📊 Configuration:")
print(f"   Current BTC: ${current_btc:,.2f}")
print(f"   Grid Range: ${lower_price:,} - ${upper_price:,}")
print(f"   Range Width: ${upper_price - lower_price:,}")
print(f"   Grids: {strategy.grid_count}")
print(f"   Grid Step: ${(strategy.grid_upper - strategy.grid_lower) / strategy.grid_count:.2f} ⭐")
print(f"   Capital per grid: ${strategy.capital_per_grid}")

# Fetch data
print(f"\n📥 Fetching data...")
ohlcv = exchange.fetch_ohlcv("BTC/USDT", "15m", limit=500)

recent_prices = [c[4] for c in ohlcv[-100:]]
price_movement = max(recent_prices) - min(recent_prices)

print(f"   ✅ Fetched {len(ohlcv)} candles")
print(f"   Recent range: ${min(recent_prices):,.0f} - ${max(recent_prices):,.0f}")
print(f"   Price movement: ${price_movement:,.2f}")
print(f"   Latest: ${ohlcv[-1][4]:,.2f}")

grid_step = (strategy.grid_upper - strategy.grid_lower) / strategy.grid_count

print(f"\n🎯 Grid vs Price Movement:")
print(f"   Grid step:      ${grid_step:.2f}")
print(f"   Price movement: ${price_movement:.2f}")
print(f"   Ratio:          {price_movement / grid_step:.1f}x")
if price_movement > grid_step * 2:
    print(f"   ✅ Price moves {price_movement/grid_step:.1f}x grid step - SHOULD WORK!")
else:
    print(f"   ⚠️  Price barely moves - might still have few signals")

# Test
print(f"\n🧪 Testing strategy...")
signals = {"BUY": 0, "SELL": 0, "HOLD": 0, "EMERGENCY_EXIT": 0}
hold_reasons = {}

for i in range(250, len(ohlcv)):
    try:
        window = ohlcv[max(0, i-250):i+1]
        signal, sl, tp, meta = strategy.analyze("BTC/USDT", window, "15m")
        signals[signal] = signals.get(signal, 0) + 1
        
        if signal == "HOLD":
            reason = meta.get('reason', 'unknown')
            hold_reasons[reason] = hold_reasons.get(reason, 0) + 1
    except Exception as e:
        print(f"   Error at candle {i}: {e}")
        break

print(f"   ✅ Tested {len(ohlcv) - 250} candles")

# Results
print(f"\n📊 Results:")
print(f"   BUY signals:   {signals['BUY']}")
print(f"   SELL signals:  {signals['SELL']}")
print(f"   HOLD signals:  {signals['HOLD']}")

if signals['BUY'] + signals['SELL'] > 0:
    print(f"\n   ✅ Total executions: {signals['BUY'] + signals['SELL']}")
    print(f"   Buy/Sell ratio: {signals['BUY']}/{signals['SELL']}")

# Grid status
status = strategy._get_grid_status()
print(f"\n📋 Grid Status:")
print(f"   Buy executions:  {status['total_buy_executions']}")
print(f"   Sell executions: {status['total_sell_executions']}")
print(f"   Inventory:       {status['total_inventory']:.6f} BTC")
print(f"   Active grids:    {status['active_grid_count']}")

if status['active_grids']:
    print(f"\n   First 5 Active Positions:")
    for i, grid in enumerate(status['active_grids'][:5]):
        value = grid['quantity'] * current_btc
        print(f"      {i+1}. ${grid['price']:,.0f}: {grid['quantity']:.6f} BTC (${value:,.2f})")

# Analysis
print("\n" + "="*80)
if signals['BUY'] + signals['SELL'] > 0:
    print("✅ SUCCESS! Grid strategy is generating signals!")
    print(f"   With 100 grids (${grid_step:.2f} spacing), we got {signals['BUY'] + signals['SELL']} executions!")
    
    if status['total_inventory'] > 0:
        inventory_value = status['total_inventory'] * current_btc
        print(f"   Current inventory value: ${inventory_value:,.2f}")
    
    print(f"\n📝 Next Steps:")
    print(f"   1. ✅ Grid strategy working with tighter grids!")
    print(f"   2. → Optimize grid count (try 50, 75, 100)")
    print(f"   3. → Run full backtest")
    print(f"   4. → Calculate profitability")
else:
    print("⚠️  Still no signals!")
    
    if hold_reasons:
        print(f"\nHOLD Reasons:")
        for reason, count in sorted(hold_reasons.items(), key=lambda x: -x[1]):
            print(f"   {reason}: {count}")
    
    print(f"\n💡 Suggestions:")
    print(f"   - Try even MORE grids (150-200)")
    print(f"   - Or make range narrower (±3-4%)")
    print(f"   - Or use different timeframe (5m instead of 15m)")

print("="*80 + "\n")
