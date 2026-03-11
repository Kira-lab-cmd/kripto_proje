#!/usr/bin/env python3
"""
Database Balance Fix Script
Checks schema and updates USDT balance to $1000
"""

from backend.database import Database

# Database path
DB_PATH = r"C:\Users\murat\Desktop\kripto_proje\crypto-trader\backend\trading_bot.db"

def main():
    print("=" * 60)
    print("DATABASE BALANCE FIX SCRIPT")
    print("=" * 60)
    print()
    
    # Connect to database
    db = Database(DB_PATH)
    print("✅ Connected to database")
    print()
    
    # 1. Check table schema
    print("📋 Table Schema (paper_balances):")
    print("-" * 60)
    cursor = db.conn.execute("PRAGMA table_info(paper_balances)")
    columns = cursor.fetchall()
    
    column_names = []
    for col in columns:
        col_dict = dict(col)
        column_names.append(col_dict['name'])
        print(f"  Column: {col_dict['name']:<20} Type: {col_dict['type']}")
    print()
    
    # 2. Check current data
    print("💾 Current Data:")
    print("-" * 60)
    cursor = db.conn.execute("SELECT * FROM paper_balances")
    rows = cursor.fetchall()
    
    for row in rows:
        row_dict = dict(row)
        print(f"  {row_dict}")
    print()
    
    # 3. Show available database methods
    print("🔧 Available Database Methods (paper/balance related):")
    print("-" * 60)
    methods = [m for m in dir(db) if 'paper' in m.lower() or 'balance' in m.lower()]
    for m in methods:
        print(f"  - {m}")
    print()
    
    # 4. Current balance
    usdt_before = db.get_paper_balance('USDT')
    print(f"💰 Current USDT Balance: ${usdt_before:.2f}")
    print()
    
    # 5. Try to update balance using different methods
    print("🔄 Attempting to update balance to $1000...")
    print("-" * 60)
    
    # Find the correct column name for balance
    # Common names: balance, amount, qty, quantity, value
    balance_column = None
    for possible_name in ['balance', 'amount', 'qty', 'quantity', 'value', 'bal']:
        if possible_name in column_names:
            balance_column = possible_name
            break
    
    if balance_column:
        print(f"✅ Found balance column: '{balance_column}'")
        
        # Try to update
        try:
            sql = f"UPDATE paper_balances SET {balance_column} = 1000.0 WHERE symbol = 'USDT'"
            db.conn.execute(sql)
            db.conn.commit()
            print(f"✅ Executed SQL: {sql}")
        except Exception as e:
            print(f"❌ SQL update failed: {e}")
    else:
        print("❌ Could not find balance column!")
        print(f"   Available columns: {column_names}")
    
    print()
    
    # 6. Try using database methods
    print("🔄 Trying database methods...")
    print("-" * 60)
    
    # Method 1: set_paper_balance
    try:
        if hasattr(db, 'set_paper_balance'):
            db.set_paper_balance('USDT', 1000.0)
            print("✅ set_paper_balance('USDT', 1000.0) successful!")
    except Exception as e:
        print(f"⚠️  set_paper_balance failed: {e}")
    
    # Method 2: update_paper_balance
    try:
        if hasattr(db, 'update_paper_balance'):
            db.update_paper_balance('USDT', 1000.0)
            print("✅ update_paper_balance('USDT', 1000.0) successful!")
    except Exception as e:
        print(f"⚠️  update_paper_balance failed: {e}")
    
    # Method 3: upsert_paper_balance
    try:
        if hasattr(db, 'upsert_paper_balance'):
            db.upsert_paper_balance('USDT', 1000.0)
            print("✅ upsert_paper_balance('USDT', 1000.0) successful!")
    except Exception as e:
        print(f"⚠️  upsert_paper_balance failed: {e}")
    
    print()
    
    # 7. Check final balance
    usdt_after = db.get_paper_balance('USDT')
    print("=" * 60)
    print("FINAL RESULT:")
    print("=" * 60)
    print(f"Before: ${usdt_before:.2f}")
    print(f"After:  ${usdt_after:.2f}")
    
    if usdt_after == 1000.0:
        print()
        print("🎉 SUCCESS! Balance updated to $1000.00!")
        print()
        print("Next steps:")
        print("  1. Start bot: python run_bot_with_rotation.py")
        print("  2. Check logs for: paper_usdt: 1000.00")
        print("  3. Grid trading should be active! 🚀")
    else:
        print()
        print("⚠️  Balance not updated. See errors above.")
        print()
        print("Manual fix:")
        print("  Check the schema output above")
        print("  Find the correct column name for balance")
        print("  Run SQL manually with correct column name")
    
    print("=" * 60)
    
    # Close database
    db.conn.close()
    print()
    print("Database connection closed.")

if __name__ == "__main__":
    main()
