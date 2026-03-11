"""
GRID STRATEGY DEBUG - WHY NO SIGNALS?
Shows exactly why signals are being blocked
"""

import os
import sys
import ccxt

print("\n" + "="*80)
print("GRID STRATEGY DEBUG - GATE ANALYSIS")
print("="*80)

# Get current price and setup
exchange = ccxt.binance()
ticker = exchange.fetch_ticker("BTC/USDT")
current_btc = ticker['last']

lower_price = int(current_btc * 0.94)
upper_price = int(current_btc * 1.06)

os.environ["GRID_LOWER_PRICE"] = str(lower_price)
os.environ["GRID_UPPER_PRICE"] = str(upper_price)
os.environ["GRID_COUNT"] = "20"
os.environ["GRID_CAPITAL_PER_LEVEL"] = "50.0"

from backend.grid_strategy import TradingStrategy

strategy = TradingStrategy()

print(f"\n📊 Grid Configuration:")
print(f"   Current BTC: ${current_btc:,.2f}")
print(f"   Grid Range: ${lower_price:,} - ${upper_price:,}")
print(f"   Grids: {strategy.grid_count}")

# Fetch data
ohlcv = exchange.fetch_ohlcv("BTC/USDT", "15m", limit=500)
print(f"\n📥 Fetched {len(ohlcv)} candles")

# Test on last 10 candles with detailed output
print("\n" + "="*80)
print("DETAILED ANALYSIS - LAST 10 CANDLES")
print("="*80)

from backend.indicators import Indicators

for i in range(len(ohlcv) - 10, len(ohlcv)):
    window = ohlcv[max(0, i-250):i+1]
    current_price = float(window[-1][4])
    
    print(f"\n📍 Candle {i - (len(ohlcv) - 10) + 1}/10:")
    print(f"   Price: ${current_price:,.2f}")
    
    # Calculate indicators
    ind = Indicators.from_ohlcv(window)
    
    # Check gates manually
    price_in_range = strategy.grid_lower <= current_price <= strategy.grid_upper
    print(f"   └─ Price in range: {price_in_range}")
    
    # ATR check
    atr_value = ind.get("atr")
    if atr_value:
        atr_pct = atr_value / current_price
        atr_ok = strategy.min_atr_pct <= atr_pct <= strategy.max_atr_pct
        print(f"   └─ ATR: ${atr_value:.2f} ({atr_pct*100:.3f}%) - OK: {atr_ok}")
        print(f"      Range: {strategy.min_atr_pct*100:.3f}% - {strategy.max_atr_pct*100:.3f}%")
    else:
        print(f"   └─ ATR: None")
    
    # Run strategy
    signal, sl, tp, meta = strategy.analyze("BTC/USDT", window, "15m")
    
    print(f"   └─ Signal: {signal}")
    print(f"   └─ Reason: {meta.get('reason', 'N/A')}")
    
    if meta.get('gates_passed') is not None:
        print(f"   └─ Gates: {meta['gates_passed']}/3 passed")
        print(f"      - Price in range: {meta.get('price_in_range')}")
        print(f"      - ATR OK: {meta.get('atr_ok')}")
        print(f"      - Volume OK: {meta.get('volume_ok')}")

# Summary
print("\n" + "="*80)
print("📊 SUMMARY")
print("="*80)

# Test full dataset
signals = {"BUY": 0, "SELL": 0, "HOLD": 0}
hold_reasons = {}

for i in range(250, len(ohlcv)):
    window = ohlcv[max(0, i-250):i+1]
    signal, sl, tp, meta = strategy.analyze("BTC/USDT", window, "15m")
    signals[signal] = signals.get(signal, 0) + 1
    
    if signal == "HOLD":
        reason = meta.get('reason', 'unknown')
        hold_reasons[reason] = hold_reasons.get(reason, 0) + 1

print(f"\nSignals:")
print(f"   BUY:  {signals['BUY']}")
print(f"   SELL: {signals['SELL']}")
print(f"   HOLD: {signals['HOLD']}")

print(f"\nHOLD Reasons:")
for reason, count in sorted(hold_reasons.items(), key=lambda x: -x[1]):
    print(f"   {reason}: {count}")

print("\n" + "="*80)
print("💡 DIAGNOSIS")
print("="*80)

if hold_reasons.get('gates_failed', 0) > 0:
    print("⚠️  PROBLEM: Gates are blocking signals!")
    print("   → Most likely ATR gate is too restrictive")
    print("   → Solution: Widen ATR range or disable gate check")
elif hold_reasons.get('no_grid_crossing', 0) > 0:
    print("⚠️  PROBLEM: No grid crossings detected!")
    print("   → Price not moving between grids")
    print("   → Solution: Make grids closer together")
else:
    print("🤔 Unknown issue - check detailed output above")

print("\n" + "="*80 + "\n")
