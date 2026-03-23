# File: backend/tests/test_database_positions.py
from pathlib import Path
import sqlite3
from backend.database import Database

def test_list_open_positions(tmp_path: Path):
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("""
        CREATE TABLE positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            entry_price REAL,
            amount REAL,
            stop_loss REAL,
            take_profit REAL,
            opened_at TEXT,
            updated_at TEXT,
            status TEXT
        )
    """)
    conn.execute("INSERT INTO positions (symbol, side, entry_price, amount, status) VALUES ('ETH/USDT','BUY',2000,0.1,'OPEN')")
    conn.execute("INSERT INTO positions (symbol, side, entry_price, amount, status) VALUES ('BTC/USDT','BUY',50000,0.01,'CLOSED')")
    conn.commit()
    conn.close()

    with Database(db_file) as db:
        rows = db.list_open_positions()
    assert len(rows) == 1
    assert rows[0]["symbol"] == "ETH/USDT"
    assert rows[0]["status"] == "OPEN"
