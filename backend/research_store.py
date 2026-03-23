# File: backend/research_store.py
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class OhlcvRow:
    symbol: str
    timeframe: str
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class ResearchStore:
    """
    Local OHLCV store (SQLite).
    Uses separate DB file (e.g. research.db) to avoid locking the trading DB.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def init_schema(self) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ohlcv (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    ts_ms INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    PRIMARY KEY (symbol, timeframe, ts_ms)
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_tf_ts ON ohlcv(timeframe, ts_ms);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_sym_tf_ts ON ohlcv(symbol, timeframe, ts_ms);")
        finally:
            conn.close()

    def upsert_rows(self, rows: Iterable[OhlcvRow]) -> int:
        conn = self._connect()
        inserted = 0
        try:
            cur = conn.cursor()
            cur.execute("BEGIN;")
            cur.executemany(
                """
                INSERT OR IGNORE INTO ohlcv(symbol,timeframe,ts_ms,open,high,low,close,volume)
                VALUES(?,?,?,?,?,?,?,?);
                """,
                [(r.symbol, r.timeframe, r.ts_ms, r.open, r.high, r.low, r.close, r.volume) for r in rows],
            )
            inserted = int(cur.rowcount or 0)
            cur.execute("COMMIT;")
        except Exception:
            try:
                conn.execute("ROLLBACK;")
            except Exception:
                pass
            raise
        finally:
            conn.close()
        return inserted

    # Backward-compat alias used by some tests/older code paths.
    def upsert_ohlcv_rows(self, *args) -> int:
        """Backward-compat helper.

        Supported call shapes:
          - upsert_ohlcv_rows(rows: Iterable[OhlcvRow])
          - upsert_ohlcv_rows(symbol: str, timeframe: str, rows: Iterable[tuple|list])
        """

        if len(args) == 1:
            rows_list = list(args[0])
            if not rows_list:
                return 0
            if isinstance(rows_list[0], OhlcvRow):
                return self.upsert_rows(rows_list)  # type: ignore[arg-type]
            raise TypeError("upsert_ohlcv_rows(rows) expects OhlcvRow objects")

        if len(args) != 3:
            raise TypeError("upsert_ohlcv_rows expects (rows) or (symbol, timeframe, rows)")

        symbol = str(args[0])
        timeframe = str(args[1])
        rows_list = list(args[2])
        if not rows_list:
            return 0
        o: list[OhlcvRow] = []
        for r in rows_list:
            t = r  # tuple-like
            o.append(
                OhlcvRow(
                    symbol=symbol,
                    timeframe=timeframe,
                    ts_ms=int(t[0]),
                    open=float(t[1]),
                    high=float(t[2]),
                    low=float(t[3]),
                    close=float(t[4]),
                    volume=float(t[5]),
                )
            )
        return self.upsert_rows(o)

    def latest_ts(self, symbol: str, timeframe: str) -> int | None:
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT MAX(ts_ms) FROM ohlcv WHERE symbol=? AND timeframe=?;",
                (symbol, timeframe),
            )
            row = cur.fetchone()
            if not row:
                return None
            v = row[0]
            return int(v) if v is not None else None
        finally:
            conn.close()

    def load(self, symbol: str, timeframe: str, since_ms: int | None = None) -> list[OhlcvRow]:
        conn = self._connect()
        try:
            if since_ms is None:
                cur = conn.execute(
                    """
                    SELECT symbol,timeframe,ts_ms,open,high,low,close,volume
                    FROM ohlcv
                    WHERE symbol=? AND timeframe=?
                    ORDER BY ts_ms ASC;
                    """,
                    (symbol, timeframe),
                )
            else:
                cur = conn.execute(
                    """
                    SELECT symbol,timeframe,ts_ms,open,high,low,close,volume
                    FROM ohlcv
                    WHERE symbol=? AND timeframe=? AND ts_ms>=?
                    ORDER BY ts_ms ASC;
                    """,
                    (symbol, timeframe, int(since_ms)),
                )

            return [OhlcvRow(*r) for r in cur.fetchall()]
        finally:
            conn.close()