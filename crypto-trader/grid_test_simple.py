"""
SIMPLE GRID STRATEGY TEST - DEBUG VERSION
Hiçbir import hatası olmadan çalışmalı
"""

import os
import sys

print("\n" + "="*80)
print("GRID STRATEGY TEST - DEBUG MODE")
print("="*80)

# Step 1: Check Python path
print("\n1️⃣ Checking Python path...")
print(f"   Current dir: {os.getcwd()}")
print(f"   Python path: {sys.path[0]}")

# Step 2: Try importing backend modules
print("\n2️⃣ Testing imports...")
try:
    from backend.grid_strategy import TradingStrategy
    print("   ✅ grid_strategy imported successfully")
except Exception as e:
    print(f"   ❌ grid_strategy import failed: {e}")
    sys.exit(1)

try:
    import ccxt
    print("   ✅ ccxt imported successfully")
except Exception as e:
    print(f"   ❌ ccxt import failed: {e}")
    print("   Install: pip install ccxt")
    sys.exit(1)

# Step 3: Create strategy
print("\n3️⃣ Creating grid strategy...")
try:
    os.environ["GRID_LOWER_PRICE"] = "90000"
    os.environ["GRID_UPPER_PRICE"] = "100000"
    os.environ["GRID_COUNT"] = "20"
    os.environ["GRID_CAPITAL_PER_LEVEL"] = "50.0"
    
    strategy = TradingStrategy()
    print(f"   ✅ Strategy created!")
    print(f"   Range: ${strategy.grid_lower:,.0f} - ${strategy.grid_upper:,.0f}")
    print(f"   Grids: {strategy.grid_count}")
except Exception as e:
    print(f"   ❌ Strategy creation failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 4: Fetch simple data from Binance
print("\n4️⃣ Fetching BTC data from Binance...")
try:
    exchange = ccxt.binance()
    
    # Fetch last 500 candles
    ohlcv = exchange.fetch_ohlcv(
        symbol="BTC/USDT",
        timeframe="15m",
        limit=500
    )
    
    print(f"   ✅ Fetched {len(ohlcv)} candles")
    print(f"   Latest close: ${ohlcv[-1][4]:,.2f}")
    print(f"   Price range: ${min(c[4] for c in ohlcv[-100:]):,.0f} - ${max(c[4] for c in ohlcv[-100:]):,.0f}")
    
except Exception as e:
    print(f"   ❌ Binance fetch failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 5: Test strategy on data
print("\n5️⃣ Testing grid strategy...")
try:
    signals = {"BUY": 0, "SELL": 0, "HOLD": 0, "ERROR": 0}
    
    # Test on last 200 candles
    for i in range(250, len(ohlcv)):
        try:
            window = ohlcv[max(0, i-250):i+1]
            signal, sl, tp, meta = strategy.analyze("BTC/USDT", window, "15m")
            signals[signal] = signals.get(signal, 0) + 1
        except Exception as e:
            signals["ERROR"] += 1
            if signals["ERROR"] == 1:  # Print first error only
                print(f"   ⚠️  First error: {e}")
    
    print(f"   ✅ Analysis complete!")
    print(f"   BUY signals:   {signals.get('BUY', 0)}")
    print(f"   SELL signals:  {signals.get('SELL', 0)}")
    print(f"   HOLD signals:  {signals.get('HOLD', 0)}")
    if signals.get('ERROR', 0) > 0:
        print(f"   ❌ Errors:     {signals['ERROR']}")
    
except Exception as e:
    print(f"   ❌ Strategy test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 6: Show grid status
print("\n6️⃣ Grid Status:")
try:
    status = strategy._get_grid_status()
    print(f"   Total buy executions:  {status['total_buy_executions']}")
    print(f"   Total sell executions: {status['total_sell_executions']}")
    print(f"   Current inventory:     {status['total_inventory']:.6f} BTC")
    print(f"   Active grids:          {status['active_grid_count']}")
    
    if status['active_grids']:
        print(f"\n   Active Grid Levels:")
        for grid in status['active_grids'][:5]:
            print(f"      ${grid['price']:,.0f}: {grid['quantity']:.6f} BTC")
except Exception as e:
    print(f"   ❌ Grid status failed: {e}")

print("\n" + "="*80)
print("✅ TEST COMPLETE!")
print("="*80 + "\n")

print("📝 Next Steps:")
if signals.get('BUY', 0) > 0 or signals.get('SELL', 0) > 0:
    print("   ✅ Grid strategy is working!")
    print("   ✅ Signals are being generated!")
    print("   → Ready for full backtest!")
else:
    print("   ⚠️  No BUY/SELL signals generated")
    print("   → Check if price is in grid range")
    print(f"   → Current price: ${ohlcv[-1][4]:,.0f}")
    print(f"   → Grid range: $90,000 - $100,000")
