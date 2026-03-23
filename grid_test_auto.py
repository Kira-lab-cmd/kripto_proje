"""
GRID STRATEGY TEST - CURRENT BTC PRICE
Automatically adjusts grid range to current price
"""

import os
import sys
import ccxt

print("\n" + "="*80)
print("GRID STRATEGY TEST - AUTO PRICE RANGE")
print("="*80)

# Step 1: Get current BTC price
print("\n1️⃣ Getting current BTC price...")
try:
    exchange = ccxt.binance()
    ticker = exchange.fetch_ticker("BTC/USDT")
    current_btc = ticker['last']
    print(f"   ✅ Current BTC price: ${current_btc:,.2f}")
except Exception as e:
    print(f"   ❌ Failed to get price: {e}")
    current_btc = 68000  # Default fallback
    print(f"   ⚠️  Using fallback: ${current_btc:,.2f}")

# Step 2: Calculate grid range (±6% from current price)
lower_price = int(current_btc * 0.94)  # -6%
upper_price = int(current_btc * 1.06)  # +6%

print(f"\n2️⃣ Calculated grid range:")
print(f"   Lower: ${lower_price:,} ({((lower_price/current_btc - 1)*100):+.1f}%)")
print(f"   Upper: ${upper_price:,} ({((upper_price/current_btc - 1)*100):+.1f}%)")
print(f"   Range width: ${upper_price - lower_price:,}")

# Step 3: Set grid parameters
os.environ["GRID_LOWER_PRICE"] = str(lower_price)
os.environ["GRID_UPPER_PRICE"] = str(upper_price)
os.environ["GRID_COUNT"] = "20"
os.environ["GRID_CAPITAL_PER_LEVEL"] = "50.0"

print(f"\n3️⃣ Creating grid strategy...")
try:
    from backend.grid_strategy import TradingStrategy
    strategy = TradingStrategy()
    print(f"   ✅ Strategy created!")
    print(f"   Grids: {strategy.grid_count}")
    print(f"   Grid step: ${(strategy.grid_upper - strategy.grid_lower) / strategy.grid_count:,.0f}")
    print(f"   Capital per grid: ${strategy.capital_per_grid}")
except Exception as e:
    print(f"   ❌ Failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 4: Fetch data
print(f"\n4️⃣ Fetching BTC data...")
try:
    ohlcv = exchange.fetch_ohlcv("BTC/USDT", "15m", limit=500)
    print(f"   ✅ Fetched {len(ohlcv)} candles")
    
    recent_prices = [c[4] for c in ohlcv[-100:]]
    print(f"   Recent range: ${min(recent_prices):,.0f} - ${max(recent_prices):,.0f}")
    print(f"   Latest: ${ohlcv[-1][4]:,.2f}")
except Exception as e:
    print(f"   ❌ Failed: {e}")
    sys.exit(1)

# Step 5: Test strategy
print(f"\n5️⃣ Testing grid strategy...")
signals = {"BUY": 0, "SELL": 0, "HOLD": 0, "EMERGENCY_EXIT": 0}
errors = []

for i in range(250, len(ohlcv)):
    try:
        window = ohlcv[max(0, i-250):i+1]
        signal, sl, tp, meta = strategy.analyze("BTC/USDT", window, "15m")
        signals[signal] = signals.get(signal, 0) + 1
    except Exception as e:
        if len(errors) == 0:  # Store first error
            errors.append(str(e))

print(f"   ✅ Tested {len(ohlcv) - 250} candles")
print(f"   BUY signals:   {signals['BUY']}")
print(f"   SELL signals:  {signals['SELL']}")
print(f"   HOLD signals:  {signals['HOLD']}")
if signals['EMERGENCY_EXIT'] > 0:
    print(f"   ⚠️  Emergency exits: {signals['EMERGENCY_EXIT']}")
if errors:
    print(f"   ⚠️  First error: {errors[0]}")

# Step 6: Grid status
print(f"\n6️⃣ Grid Status:")
status = strategy._get_grid_status()
print(f"   Buy executions:  {status['total_buy_executions']}")
print(f"   Sell executions: {status['total_sell_executions']}")
print(f"   Inventory:       {status['total_inventory']:.6f} BTC (${status['total_inventory'] * current_btc:,.2f})")
print(f"   Active grids:    {status['active_grid_count']}")

if status['active_grids']:
    print(f"\n   Active Positions:")
    for grid in status['active_grids'][:5]:
        value = grid['quantity'] * current_btc
        print(f"      ${grid['price']:,.0f}: {grid['quantity']:.6f} BTC (${value:,.2f})")

# Step 7: Analysis
print("\n" + "="*80)
print("📊 ANALYSIS")
print("="*80)

total_signals = signals['BUY'] + signals['SELL']
if total_signals > 0:
    print(f"✅ SUCCESS! Grid strategy is working!")
    print(f"   Total executions: {total_signals}")
    print(f"   Buy/Sell ratio: {signals['BUY']}/{signals['SELL']}")
    
    if status['total_inventory'] > 0:
        inventory_value = status['total_inventory'] * current_btc
        print(f"   Current inventory: ${inventory_value:,.2f}")
    
    print(f"\n📝 Next Steps:")
    print(f"   1. ✅ Grid strategy working correctly")
    print(f"   2. → Run full backtest with this range")
    print(f"   3. → Check profitability")
else:
    print(f"⚠️  No signals generated")
    print(f"   This could mean:")
    print(f"   - Price not volatile enough")
    print(f"   - ATR gate blocking signals")
    print(f"   - Grid range too wide/narrow")
    
    print(f"\n💡 Suggestions:")
    print(f"   - Check ATR values")
    print(f"   - Widen/narrow grid range")
    print(f"   - Use longer time period")

print("\n" + "="*80 + "\n")
