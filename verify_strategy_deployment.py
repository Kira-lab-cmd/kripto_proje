#!/usr/bin/env python3
"""
Verify that grid strategy is properly deployed
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "backend"))

from backend.strategy import TradingStrategy

print("="*80)
print("STRATEGY DEPLOYMENT VERIFICATION")
print("="*80)
print()

# Create strategy instance
strategy = TradingStrategy()

print("Strategy Configuration:")
print(f"  Grid Lower: ${strategy.grid_lower:,.0f}")
print(f"  Grid Upper: ${strategy.grid_upper:,.0f}")
print(f"  Grid Count: {strategy.grid_count}")
print()

# Check if get_signal exists and has correct signature
import inspect

print("Checking get_signal() method:")
if hasattr(strategy, 'get_signal'):
    print("  ✅ get_signal() exists")
    
    # Get signature
    sig = inspect.signature(strategy.get_signal)
    print(f"  Parameters: {list(sig.parameters.keys())}")
    
    # Try calling it with dummy data
    dummy_ohlcv = [[1000000, 90000, 90100, 89900, 90050, 1000]] * 250
    
    try:
        result = strategy.get_signal(
            ohlcv_data=dummy_ohlcv,
            sentiment_score=0.0,
            symbol="BTC/USDT",
            trend_dir_1h="NEUTRAL"
        )
        
        print(f"  ✅ get_signal() callable")
        print(f"  Return keys: {list(result.keys())}")
        
        # Check critical fields
        has_stop_loss = 'stop_loss' in result
        has_take_profit = 'take_profit' in result
        has_signal = 'signal' in result
        
        print()
        print("Critical Fields Check:")
        print(f"  'signal' field: {'✅' if has_signal else '❌'}")
        print(f"  'stop_loss' field: {'✅' if has_stop_loss else '❌'}")
        print(f"  'take_profit' field: {'✅' if has_take_profit else '❌'}")
        
        if not (has_signal and has_stop_loss and has_take_profit):
            print()
            print("  ❌ MISSING CRITICAL FIELDS!")
            print("  Backtest will skip all trades!")
        else:
            print()
            print("  ✅ All critical fields present!")
            print()
            print("  Sample return:")
            print(f"    signal: {result.get('signal')}")
            print(f"    stop_loss: {result.get('stop_loss')}")
            print(f"    take_profit: {result.get('take_profit')}")
            print(f"    score: {result.get('score')}")
            
    except Exception as e:
        print(f"  ❌ get_signal() call failed: {e}")
else:
    print("  ❌ get_signal() NOT FOUND!")
    print("  This is why backtest has 0 trades!")

print()

# Check if analyze() exists
if hasattr(strategy, 'analyze'):
    print("✅ analyze() method exists")
else:
    print("❌ analyze() method NOT FOUND!")

print()
print("="*80)
print("CONCLUSION:")
print("="*80)

if hasattr(strategy, 'get_signal'):
    try:
        dummy_ohlcv = [[1000000, 90000, 90100, 89900, 90050, 1000]] * 250
        result = strategy.get_signal(
            ohlcv_data=dummy_ohlcv,
            sentiment_score=0.0,
            symbol="BTC/USDT",
            trend_dir_1h="NEUTRAL"
        )
        
        if 'signal' in result and 'stop_loss' in result and 'take_profit' in result:
            print("✅ Grid strategy PROPERLY DEPLOYED!")
            print("   All required fields present.")
            print()
            print("   If backtest still shows 0 trades, the issue is:")
            print("   - Grid crossing detection logic")
            print("   - Or gates always failing")
        else:
            print("❌ Grid strategy HAS BUGS!")
            print("   Missing required fields.")
            print()
            print("   ACTION REQUIRED:")
            print("   - Re-copy grid_strategy_FIXED_FINAL.py to backend/strategy.py")
            print("   - Clear cache")
            print("   - Restart")
    except Exception as e:
        print(f"❌ Strategy has errors: {e}")
else:
    print("❌ Grid strategy NOT DEPLOYED!")
    print()
    print("   ACTION REQUIRED:")
    print("   - Copy grid_strategy_FIXED_FINAL.py to backend/strategy.py")
    print("   - Clear cache")
    print("   - Restart backtest")

print("="*80)
