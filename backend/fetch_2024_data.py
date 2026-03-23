"""
Fetch 2024 Historical Data and Save to research.db

Usage:
    python -m backend.fetch_2024_data
"""

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.binance_service import BinanceService
from backend.research_store import ResearchStore, OhlcvRow


def fetch_and_save_2024_data():
    """Fetch 2024 data from Binance and save to research.db"""
    
    print("\n" + "="*80)
    print("FETCHING 2024 HISTORICAL DATA FROM BINANCE")
    print("="*80 + "\n")
    
    # Initialize services
    print("🔌 Connecting to Binance...")
    binance = BinanceService()
    print("  ✅ Connected!\n")
    
    research_db_path = os.getenv("RESEARCH_DB_PATH", "research.db").strip() or "research.db"
    print(f"💾 Using database: {research_db_path}\n")
    
    store = ResearchStore(db_path=research_db_path)
    store.init_schema()
    
    # Date range
    start_date = "2024-10-01"
    end_date = "2024-12-31"
    symbols = ["BTC/USDT", "ETH/USDT"]
    timeframe = "15m"
    
    print(f"📅 Period: {start_date} to {end_date}")
    print(f"🪙 Symbols: {symbols}")
    print(f"⏱️  Timeframe: {timeframe}\n")
    
    # Parse dates
    start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
    
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    
    total_saved = 0
    
    for symbol in symbols:
        print(f"\n{'='*80}")
        print(f"📊 Processing {symbol}")
        print(f"{'='*80}\n")
        
        # Fetch data
        print(f"  📥 Fetching from Binance...")
        
        all_candles = []
        current_start = start_ms
        
        while current_start < end_ms:
            candles = binance.exchange.fetch_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                since=current_start,
                limit=1000
            )
            
            if not candles:
                break
            
            # Filter candles within date range
            filtered_candles = [c for c in candles if c[0] <= end_ms]
            all_candles.extend(filtered_candles)
            
            # Move to next chunk
            last_timestamp = int(candles[-1][0])
            current_start = last_timestamp + 1
            
            # Show progress
            progress_dt = datetime.fromtimestamp(last_timestamp / 1000, tz=timezone.utc)
            print(f"    Progress: {progress_dt.strftime('%Y-%m-%d')}...", end='\r')
            
            # Safety check
            if len(all_candles) > 50000:
                print(f"\n  ⚠️  Reached 50k candles limit")
                break
        
        print(f"  ✅ Fetched {len(all_candles)} candles" + " "*30)
        
        # Convert to OhlcvRow format
        rows = []
        for candle in all_candles:
            rows.append(OhlcvRow(
                symbol=symbol,
                timeframe=timeframe,
                ts_ms=int(candle[0]),
                open=float(candle[1]),
                high=float(candle[2]),
                low=float(candle[3]),
                close=float(candle[4]),
                volume=float(candle[5]),
            ))
        
        # Save to database
        print(f"  💾 Saving to research.db...")
        saved = store.upsert_rows(rows)
        print(f"  ✅ Saved {saved} rows to database\n")
        
        total_saved += saved
    
    print("\n" + "="*80)
    print("✅ DATA FETCH COMPLETE!")
    print("="*80 + "\n")
    print(f"Total rows saved: {total_saved}")
    print(f"Database: {research_db_path}\n")
    print("📝 Next Step:")
    print("  Run backtest on 2024 data:")
    print(f"  python -m backend.walkforward --start 2024-10-01 --end 2024-12-31 ...")
    print()


if __name__ == "__main__":
    fetch_and_save_2024_data()
