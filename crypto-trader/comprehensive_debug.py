#!/usr/bin/env python3
"""
Test grid strategy EXACTLY like backtest does
"""
import sys
import sqlite3
import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "backend"))

from backend.strategy import TradingStrategy

print("="*80)
print("COMPREHENSIVE GRID STRATEGY DEBUG")
print("="*80)
print()

# Create strategy
strategy = TradingStrategy()

print("1️⃣ STRATEGY CONFIGURATION:")
print(f"   Grid Lower: ${strategy.grid_lower:,.0f}")
print(f"   Grid Upper: ${strategy.grid_upper:,.0f}")
print(f"   Grid Count: {strategy.grid_count}")
print(f"   Capital per grid: ${strategy.capital_per_grid:,.2f}")
print()

# Get December 2025 data
conn = sqlite3.connect('backend/research.db')
cursor = conn.cursor()

dec_start = int(datetime.datetime(2025, 12, 1).timestamp() * 1000)
dec_end = int(datetime.datetime(2025, 12, 31, 23, 59).timestamp() * 1000)

cursor.execute("""
    SELECT ts_ms, open, high, low, close, volume
    FROM ohlcv 
    WHERE symbol='BTC/USDT' 
    AND timeframe='15m'
    AND ts_ms BETWEEN ? AND ?
    ORDER BY ts_ms
""", (dec_start, dec_end))

rows = cursor.fetchall()
conn.close()

print(f"2️⃣ DATA LOADED:")
print(f"   {len(rows):,} candles from research.db")
print(f"   First: {datetime.datetime.fromtimestamp(rows[0][0]/1000)}")
print(f"   Last: {datetime.datetime.fromtimestamp(rows[-1][0]/1000)}")
print()

# Test EXACTLY like backtest
print("3️⃣ TESTING WITH BACKTEST APPROACH:")
print()

warmup = 200
buy_count = 0
sell_count = 0
hold_count = 0

# Track first 5 signals
signals_log = []

for i in range(warmup, min(warmup + 500, len(rows))):  # Test 500 candles
    window_rows = rows[i-warmup:i+1]
    ohlcv = [[row[0], row[1], row[2], row[3], row[4], row[5]] for row in window_rows]
    
    # Call get_signal EXACTLY like backtest
    res = strategy.get_signal(
        ohlcv_data=ohlcv,
        sentiment_score=0.0,
        symbol="BTC/USDT",
        trend_dir_1h="NEUTRAL"
    )
    
    signal = res.get("signal", "HOLD")
    
    # Log all signals for debugging
    dt = datetime.datetime.fromtimestamp(ohlcv[-1][0]/1000)
    price = ohlcv[-1][4]
    
    if signal == "BUY":
        buy_count += 1
        if buy_count <= 5:
            signals_log.append({
                "type": "BUY",
                "time": dt,
                "price": price,
                "res": res
            })
    elif signal == "SELL":
        sell_count += 1
        if sell_count <= 5:
            signals_log.append({
                "type": "SELL", 
                "time": dt,
                "price": price,
                "res": res
            })
    else:
        hold_count += 1
        if hold_count <= 5:
            signals_log.append({
                "type": "HOLD",
                "time": dt,
                "price": price,
                "res": res
            })

print("4️⃣ RESULTS:")
print(f"   BUY signals: {buy_count}")
print(f"   SELL signals: {sell_count}")
print(f"   HOLD signals: {hold_count}")
print(f"   Total: {buy_count + sell_count + hold_count}")
print()

# Show first signals
print("5️⃣ FIRST 5 SIGNALS OF EACH TYPE:")
print()

for sig in signals_log[:15]:  # Show first 15
    print(f"   {sig['type']:4s} @ {sig['time'].strftime('%Y-%m-%d %H:%M')} | ${sig['price']:,.2f}")
    res = sig['res']
    
    # Check critical fields
    has_sl = 'stop_loss' in res
    has_tp = 'take_profit' in res
    
    if sig['type'] in ['BUY', 'SELL']:
        print(f"        stop_loss: {'✅ ' + str(res.get('stop_loss', 0))[:10] if has_sl else '❌ MISSING'}")
        print(f"        take_profit: {'✅ ' + str(res.get('take_profit', 0))[:10] if has_tp else '❌ MISSING'}")
        print(f"        score: {res.get('score', 0)}")
        print(f"        reason: {res.get('entry_reason', 'N/A')}")
        
        if not has_sl or not has_tp:
            print(f"        ⚠️  BACKTEST WILL SKIP THIS TRADE!")
    else:
        print(f"        reason: {res.get('reason', 'N/A')}")
    
    print()

print("="*80)
print("6️⃣ DIAGNOSTIC CONCLUSION:")
print("="*80)
print()

if buy_count == 0 and sell_count == 0:
    print("❌ NO TRADES GENERATED")
    print()
    print("   Possible causes:")
    print("   1. Grid crossing logic not triggering")
    print("   2. last_price issue")
    print("   3. Gates always failing")
    print()
    
    # Check a HOLD signal in detail
    for sig in signals_log:
        if sig['type'] == 'HOLD':
            print("   Analyzing first HOLD signal:")
            print(f"   Reason: {sig['res'].get('reason', 'unknown')}")
            print(f"   Price: ${sig['res'].get('current_price', 0):,.2f}")
            print(f"   Grid range: ${strategy.grid_lower:,.0f} - ${strategy.grid_upper:,.0f}")
            break
            
elif buy_count > 0 or sell_count > 0:
    print(f"✅ TRADES GENERATED: {buy_count + sell_count}")
    print()
    
    # Check if they have required fields
    has_required_fields = True
    for sig in signals_log:
        if sig['type'] in ['BUY', 'SELL']:
            if 'stop_loss' not in sig['res'] or 'take_profit' not in sig['res']:
                has_required_fields = False
                break
    
    if has_required_fields:
        print("   ✅ All BUY/SELL signals have stop_loss and take_profit")
        print()
        print("   🎯 Strategy IS working correctly!")
        print()
        print("   If backtest still shows 0 trades:")
        print("   → Backtest infrastructure issue")
        print("   → Or different data being used")
        print("   → Or caching issue persists")
    else:
        print("   ❌ Some signals missing stop_loss or take_profit")
        print()
        print("   → These trades will be skipped by backtest!")
        print("   → Fix: Ensure get_signal() always returns these fields")

print()
print("="*80)
