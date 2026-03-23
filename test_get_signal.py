"""
TEST: get_signal() backtest formatı
"""
import os
import ccxt

# Grid parameters
os.environ["GRID_LOWER_PRICE"] = "63700"
os.environ["GRID_UPPER_PRICE"] = "71800"
os.environ["GRID_COUNT"] = "100"
os.environ["GRID_CAPITAL_PER_LEVEL"] = "10.0"
os.environ["MIN_ATR_PCT"] = "0.001"

from backend.strategy import TradingStrategy

print("="*80)
print("GET_SIGNAL() BACKTEST FORMAT TEST")
print("="*80)

# Create strategy
strategy = TradingStrategy()

# Fetch data
exchange = ccxt.binance()
ohlcv = exchange.fetch_ohlcv("BTC/USDT", "15m", limit=500)

print(f"\n✅ Strategy created")
print(f"✅ Fetched {len(ohlcv)} candles")

# Test get_signal() 
print(f"\n🧪 Testing get_signal() on last 10 candles...")
print()

for i in range(len(ohlcv) - 10, len(ohlcv)):
    window = ohlcv[max(0, i-250):i+1]
    
    # Call get_signal (backtest format)
    result = strategy.get_signal(
        ohlcv_data=window,
        sentiment_score=0.0,
        symbol="BTC/USDT",
        trend_dir_1h=None
    )
    
    signal = result.get("signal", "UNKNOWN")
    score = result.get("score", 0)
    price = result.get("current_price", 0)
    reason = result.get("reason", "N/A")
    
    print(f"Candle {i - (len(ohlcv) - 10) + 1}/10: Price ${price:,.0f} → Signal: {signal} (score: {score})")
    
    if signal != "HOLD":
        print(f"   ✅ Entry reason: {result.get('entry_reason', 'N/A')}")
        print(f"   Stop: ${result.get('stop_price', 0):,.0f}")
        print(f"   Target: ${result.get('target_price', 0):,.0f}")
    elif reason != "N/A":
        print(f"   └─ Reason: {reason}")

print()
print("="*80)
print("CRITICAL CHECKS:")
print("="*80)

# Check if we're getting any BUY/SELL signals
test_signals = {"BUY": 0, "SELL": 0, "HOLD": 0}
for i in range(250, len(ohlcv)):
    window = ohlcv[max(0, i-250):i+1]
    result = strategy.get_signal(
        ohlcv_data=window,
        sentiment_score=0.0,
        symbol="BTC/USDT"
    )
    signal = result.get("signal", "HOLD")
    test_signals[signal] = test_signals.get(signal, 0) + 1

print(f"\n250 candle test:")
print(f"   BUY signals:  {test_signals['BUY']}")
print(f"   SELL signals: {test_signals['SELL']}")
print(f"   HOLD signals: {test_signals['HOLD']}")
print()

if test_signals['BUY'] + test_signals['SELL'] > 0:
    print("✅ get_signal() IS WORKING!")
    print("   → Backtest should have trades")
    print("   → Format is correct")
else:
    print("❌ get_signal() returning only HOLD!")
    print("   → Check grid parameters")
    print("   → Check gates")

print("="*80)
