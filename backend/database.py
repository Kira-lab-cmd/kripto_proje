from __future__ import annotations
import atexit
import json
import weakref
import json
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

from .utils_symbols import normalize_symbol


logger = logging.getLogger(__name__)
TR_TZ = ZoneInfo("Europe/Istanbul")
UTC_TZ = ZoneInfo("UTC")
IsoTimespec = Literal["auto", "hours", "minutes", "seconds", "milliseconds", "microseconds"]


@dataclass(frozen=True)
class DayRange:
    start_key: str
    end_key: str
    start_tr: datetime
    end_tr: datetime
    start_utc: datetime
    end_utc: datetime


def _format_trade_timestamp_key(dt_aware_utc: datetime, sample_ts: str | None = None) -> str:
    """
    Match the DB's timestamp text shape:
      - 2026-02-28T10:15:00
      - 2026-02-28 10:15:00
      - optional microseconds
    Defaults to the current app writer style: ISO with "T" and microseconds.
    """
    if dt_aware_utc.tzinfo is None:
        raise ValueError("dt_aware_utc must be timezone-aware UTC")

    dt_naive = dt_aware_utc.astimezone(UTC_TZ).replace(tzinfo=None)
    ts = (sample_ts or "").strip()
    use_space = " " in ts and "T" not in ts
    use_microseconds = "." in ts or not ts

    if use_space:
        fmt = "%Y-%m-%d %H:%M:%S.%f" if use_microseconds else "%Y-%m-%d %H:%M:%S"
        return dt_naive.strftime(fmt)

    return dt_naive.isoformat(timespec="microseconds" if use_microseconds else "seconds")


def _tr_today_range_to_utc_naive_iso_keys(
    now_tr: datetime | None = None,
    sample_ts: str | None = None,
) -> DayRange:
    """
    DB: trades.timestamp is TEXT and stored as UTC-naive ISO string.
    We want "today" in TR timezone => [TR 00:00, next day TR 00:00) converted to UTC,
    then converted to the DB's naive ISO key format.
    """
    if now_tr is None:
        now_tr = datetime.now(tz=TR_TZ)
    if now_tr.tzinfo is None:
        raise ValueError("now_tr must be timezone-aware (Europe/Istanbul)")

    start_tr = now_tr.replace(hour=0, minute=0, second=0, microsecond=0)
    end_tr = start_tr + timedelta(days=1)

    start_utc = start_tr.astimezone(UTC_TZ)
    end_utc = end_tr.astimezone(UTC_TZ)

    start_key = _format_trade_timestamp_key(start_utc, sample_ts=sample_ts)
    end_key = _format_trade_timestamp_key(end_utc, sample_ts=sample_ts)

    return DayRange(
        start_key=start_key,
        end_key=end_key,
        start_tr=start_tr,
        end_tr=end_tr,
        start_utc=start_utc,
        end_utc=end_utc,
    )


def utc_naive_iso_now(timespec: IsoTimespec = "seconds") -> str:
    """
    DB contract: UTC-naive ISO string (no tzinfo).
    """
    return datetime.now(tz=UTC_TZ).replace(tzinfo=None).isoformat(timespec=timespec)


class Database:
    _instance = None                # Singleton instance
    _lock = threading.Lock()        # Thread safety

    def __new__(cls, db_path: str = ""):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance        # ✅ Database objesi döner

    def __init__(self, db_path: str = ""):
        # Zaten başlatıldıysa tekrar başlatma
        if getattr(self, "_initialized", False):
            return
        self._initialized = True

        
    _atexit_registered: bool = False

    """
    SQLite database wrapper (thread-safe with a process-local lock).

    IMPORTANT:
    - This class uses a simple threading.Lock to serialize writes within a single process.
    - Do NOT nest .transaction() calls. Internal helper methods accept a cursor to avoid nested locks.
    - For multi-worker production use, migrate to PostgreSQL.
    """

    def __init__(self, db_path: str = "trading_bot.db") -> None:
        _given = Path(db_path)
        if not _given.is_absolute() and len(_given.parts) == 1:
            _base = Path(__file__).parent
            self.db_path = (_base / _given).resolve()
        else:
            self.db_path = _given.resolve()
        
        self.lock = threading.Lock()
        logger.info(f"✅ DB path resolved: {self.db_path}")
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        # Track instances to ensure connections are closed at interpreter shutdown.
        
        if not Database._atexit_registered:
            atexit.register(Database._close_all_instances)
            Database._atexit_registered = True

    def __del__(self) -> None:
        # Best-effort cleanup to avoid noisy ResourceWarning in tests.
        try:
            self.close_db()
        except Exception:
            pass


    @staticmethod
    def _close_all_instances() -> None:
        # Best-effort shutdown cleanup (prevents noisy ResourceWarning on interpreter exit).
        if Database._instance is not None:
            try:
                if hasattr(Database._instance, 'conn') and Database._instance.conn:
                    Database._instance.conn.close()
            except Exception:
                pass

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close_db()
        return False

    def connect(self) -> None:
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def close_db(self) -> None:
        conn = getattr(self, "conn", None)
        if conn is None:
            return
        try:
            conn.close()
        finally:
            self.conn = None

    @contextmanager
    def transaction(self):
        if not self.conn:
            raise RuntimeError("DB not connected")
        with self.lock:
            cur = self.conn.cursor()
            try:
                yield cur
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    @staticmethod
    def _utc_iso() -> str:
        # datetime.utcnow() is deprecated in Python 3.13+.
        return datetime.now(tz=UTC_TZ).replace(tzinfo=None).isoformat(timespec="seconds")

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    @staticmethod
    def _effective_trade_pnl_expr(columns: set[str]) -> str | None:
        candidates: list[str] = []
        if "net_pnl" in columns:
            candidates.append("CAST(net_pnl AS REAL)")
        if "realized_pnl" in columns:
            candidates.append("CAST(realized_pnl AS REAL)")
        if "pnl" in columns:
            candidates.append("CAST(pnl AS REAL)")
        if not candidates:
            return None
        return "COALESCE(" + ", ".join(candidates) + ", 0.0)"

    @staticmethod
    def _effective_trade_pnl_value(row: sqlite3.Row | dict[str, Any]) -> float | None:
        for key in ("net_pnl", "realized_pnl", "pnl"):
            value = row[key] if isinstance(row, sqlite3.Row) else row.get(key)
            if value is not None:
                try:
                    return float(value)
                except Exception:
                    continue
        return None

    @staticmethod
    def _json_safe_profit_factor(gross_profit_sum: float, gross_loss_sum: float) -> float | str:
        if gross_loss_sum > 0:
            return float(gross_profit_sum / gross_loss_sum)
        if gross_profit_sum > 0:
            return "inf"
        return 0.0

    @staticmethod
    def _json_dumps(value: Any) -> str | None:
        if value is None:
            return None
        try:
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        except Exception:
            return None

    @staticmethod
    def _json_loads(value: Any, default: Any) -> Any:
        if value in (None, ""):
            return default
        try:
            return json.loads(str(value))
        except Exception:
            return default

    @staticmethod
    def _parse_stored_timestamp(value: str | None) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None

    @staticmethod
    def _session_bucket_label_from_hour(hour: int) -> str:
        start = max(0, min(21, (int(hour) // 3) * 3))
        end = start + 3
        return f"{start:02d}-{end:02d}"

    def _session_bucket_label(self, timestamp_text: str | None) -> str:
        dt = self._parse_stored_timestamp(timestamp_text)
        if dt is None:
            return "unknown"
        return self._session_bucket_label_from_hour(dt.hour)

    @staticmethod
    def _default_universe_next_rebuild_at() -> str:
        days = max(1, int(os.getenv("UNIVERSE_REBUILD_DAYS", "14") or "14"))
        return (datetime.now(tz=UTC_TZ) + timedelta(days=days)).replace(tzinfo=None).isoformat(timespec="seconds")

    # -----------------------------
    # Schema
    # -----------------------------
    def _create_tables(self) -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")

        cur = self.conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                amount REAL NOT NULL,
                price REAL NOT NULL,
                cost REAL NOT NULL,
                fee REAL DEFAULT 0,
                realized_pnl REAL DEFAULT 0,
                strategy_name TEXT,
                entry_reason TEXT,
                exit_reason TEXT,
                regime TEXT,
                atr_pct REAL,
                dir_1h TEXT,
                entry_price REAL,
                exit_price REAL,
                entry_cost REAL,
                exit_cost REAL,
                entry_fee REAL,
                exit_fee REAL,
                total_fees REAL,
                gross_pnl REAL,
                net_pnl REAL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                symbol TEXT PRIMARY KEY,
                amount REAL NOT NULL,
                entry_price REAL NOT NULL,
                cost REAL NOT NULL,
                updated_at TEXT NOT NULL,
                stop_loss REAL,
                take_profit REAL,
                highest_price REAL,
                opened_at TEXT,
                strategy_name TEXT,
                entry_reason TEXT,
                regime TEXT,
                atr_pct REAL,
                dir_1h TEXT,
                entry_fee_total REAL DEFAULT 0
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                is_enabled INTEGER NOT NULL DEFAULT 1,
                risk_multiplier REAL NOT NULL DEFAULT 1.0,
                updated_at TEXT NOT NULL
            )
            """
        )

        # Universe selection state (Binance-only)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS universe_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                symbols_csv TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                next_rebuild_at TEXT,
                last_reason TEXT
            )
            """
        )

        cur.execute("SELECT id FROM bot_state WHERE id = 1")
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO bot_state (id, is_enabled, risk_multiplier, updated_at) VALUES (1, 1, 1.0, ?)",
                (self._utc_iso(),),
            )

        cur.execute("SELECT id FROM universe_state WHERE id = 1")
        if cur.fetchone() is None:
            # default to .env UNIVERSE_SYMBOLS if present, else safe core
            default_syms = os.getenv(
                "UNIVERSE_SYMBOLS",
                "BTC/USDT,ETH/USDT,SOL/USDT,XRP/USDT,BNB/USDT,TRX/USDT",
            )
            next_rebuild_at = self._default_universe_next_rebuild_at()
            cur.execute(
                "INSERT INTO universe_state (id, symbols_csv, updated_at, next_rebuild_at, last_reason) VALUES (1, ?, ?, ?, 'INIT')",
                (default_syms, self._utc_iso(), next_rebuild_at),
            )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sentiment_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                score REAL NOT NULL,
                analyzed INTEGER NOT NULL DEFAULT 0,
                important INTEGER NOT NULL DEFAULT 0,
                pos_votes INTEGER NOT NULL DEFAULT 0,
                neg_votes INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL DEFAULT 'cryptopanic'
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sent_snap_symbol_ts
            ON sentiment_snapshots(symbol, ts)
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_balances (
                asset TEXT PRIMARY KEY,
                free REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_orders (
                id TEXT PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                amount REAL NOT NULL,
                mid_price REAL NOT NULL,
                exec_price REAL NOT NULL,
                fee REAL NOT NULL,
                slippage_bps REAL NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                strategy_name TEXT,
                entry_reason TEXT,
                exit_reason TEXT,
                regime TEXT,
                atr_pct REAL,
                dir_1h TEXT,
                entry_price REAL,
                exit_price REAL,
                entry_cost REAL,
                exit_cost REAL,
                entry_fee REAL,
                exit_fee REAL,
                total_fees REAL,
                gross_pnl REAL,
                net_pnl REAL,
                error TEXT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_paper_orders_symbol_created ON paper_orders(symbol, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_paper_orders_status_created ON paper_orders(status, created_at)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                strategy_name TEXT,
                decision TEXT NOT NULL,
                signal TEXT NOT NULL,
                score REAL DEFAULT 0,
                buy_threshold REAL,
                gap_to_threshold REAL,
                regime TEXT,
                dir_1h TEXT,
                atr_pct REAL,
                price REAL,
                blocked_reason TEXT,
                trade_blocked INTEGER NOT NULL DEFAULT 0,
                exec_blocked INTEGER NOT NULL DEFAULT 0,
                gate_status_json TEXT,
                hold_fail_reasons_json TEXT,
                reason TEXT,
                risk_multiplier REAL,
                corr_factor REAL,
                corr_reason TEXT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_audit_symbol_ts ON signal_audit(symbol, timestamp)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_audit_signal_ts ON signal_audit(signal, timestamp)")

        def _ensure_columns(table: str, column_defs: list[str]) -> None:
            cur.execute(f"PRAGMA table_info({table})")
            existing = {str(row["name"]) for row in cur.fetchall()}
            for column_def in column_defs:
                column_name = column_def.split()[0]
                if column_name not in existing:
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")
                    existing.add(column_name)

        _ensure_columns(
            "trades",
            [
                "strategy_name TEXT",
                "entry_reason TEXT",
                "exit_reason TEXT",
                "regime TEXT",
                "atr_pct REAL",
                "dir_1h TEXT",
                "entry_price REAL",
                "exit_price REAL",
                "entry_cost REAL",
                "exit_cost REAL",
                "entry_fee REAL",
                "exit_fee REAL",
                "total_fees REAL",
                "gross_pnl REAL",
                "net_pnl REAL",
            ],
        )
        _ensure_columns(
            "positions",
            [
                "strategy_name TEXT",
                "entry_reason TEXT",
                "regime TEXT",
                "atr_pct REAL",
                "dir_1h TEXT",
                "entry_fee_total REAL DEFAULT 0",
            ],
        )
        _ensure_columns(
            "paper_orders",
            [
                "strategy_name TEXT",
                "entry_reason TEXT",
                "exit_reason TEXT",
                "regime TEXT",
                "atr_pct REAL",
                "dir_1h TEXT",
                "entry_price REAL",
                "exit_price REAL",
                "entry_cost REAL",
                "exit_cost REAL",
                "entry_fee REAL",
                "exit_fee REAL",
                "total_fees REAL",
                "gross_pnl REAL",
                "net_pnl REAL",
            ],
        )
        _ensure_columns(
            "signal_audit",
            [
                "strategy_name TEXT",
                "decision TEXT",
                "signal TEXT",
                "score REAL DEFAULT 0",
                "buy_threshold REAL",
                "gap_to_threshold REAL",
                "regime TEXT",
                "dir_1h TEXT",
                "atr_pct REAL",
                "price REAL",
                "blocked_reason TEXT",
                "trade_blocked INTEGER NOT NULL DEFAULT 0",
                "exec_blocked INTEGER NOT NULL DEFAULT 0",
                "gate_status_json TEXT",
                "hold_fail_reasons_json TEXT",
                "reason TEXT",
                "risk_multiplier REAL",
                "corr_factor REAL",
                "corr_reason TEXT",
            ],
        )

        # ✅ NEW: coin_profiles (coin-specific strategy knobs)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS coin_profiles (
                symbol TEXT PRIMARY KEY,
                buy_threshold REAL NOT NULL,
                sell_threshold REAL NOT NULL,
                min_volume_ratio REAL NOT NULL,
                min_atr_pct REAL NOT NULL,
                max_atr_pct REAL NOT NULL,
                downtrend_buy_penalty REAL NOT NULL,
                uptrend_buy_boost REAL NOT NULL,
                risk_mult REAL NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        # ✅ Grid state persistence (restart-safe grid levels)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS grid_state (
                symbol      TEXT PRIMARY KEY,
                state_json  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            )
            """
        )

        self.conn.commit()

        try:
            default_usdt = float(os.getenv("PAPER_USDT", "100"))
        except Exception:
            default_usdt = 100.0
        self.ensure_paper_asset("USDT", initial_free=default_usdt)

    # -----------------------------
    # Internal helpers (NO nested transactions)
    # -----------------------------
    def _ensure_paper_asset_cur(self, cur: sqlite3.Cursor, asset: str, initial_free: float = 0.0) -> None:
        asset_u = asset.upper().strip()
        cur.execute("SELECT asset FROM paper_balances WHERE asset = ?", (asset_u,))
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO paper_balances (asset, free, updated_at) VALUES (?, ?, ?)",
                (asset_u, float(initial_free), self._utc_iso()),
            )

    def _get_paper_balance_cur(self, cur: sqlite3.Cursor, asset: str) -> float:
        asset_u = asset.upper().strip()
        cur.execute("SELECT free FROM paper_balances WHERE asset = ?", (asset_u,))
        r = cur.fetchone()
        return float(r["free"]) if r else 0.0

    def _set_paper_balance_cur(self, cur: sqlite3.Cursor, asset: str, free: float) -> None:
        asset_u = asset.upper().strip()
        cur.execute(
            """
            INSERT INTO paper_balances (asset, free, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(asset) DO UPDATE SET free=excluded.free, updated_at=excluded.updated_at
            """,
            (asset_u, float(free), self._utc_iso()),
        )

    # -----------------------------
    # Bot state
    # -----------------------------
    def get_bot_state(self) -> dict[str, Any]:
        if not self.conn:
            raise RuntimeError("DB not connected")
        cur = self.conn.cursor()
        cur.execute("SELECT is_enabled, risk_multiplier, updated_at FROM bot_state WHERE id = 1")
        row = cur.fetchone()
        return {
            "is_enabled": bool(row["is_enabled"]),
            "risk_multiplier": float(row["risk_multiplier"]),
            "updated_at": row["updated_at"],
        }

    def set_bot_enabled(self, enabled: bool) -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")
        with self.transaction():
            cur = self.conn.cursor()
            cur.execute(
                "UPDATE bot_state SET is_enabled = ?, updated_at = ? WHERE id = 1",
                (1 if enabled else 0, self._utc_iso()),
            )

    def set_risk_multiplier(self, multiplier: float) -> None:
        multiplier_f = max(0.0, min(1.0, float(multiplier)))
        if not self.conn:
            raise RuntimeError("DB not connected")
        with self.transaction():
            cur = self.conn.cursor()
            cur.execute(
                "UPDATE bot_state SET risk_multiplier = ?, updated_at = ? WHERE id = 1",
                (multiplier_f, self._utc_iso()),
            )

    # -----------------------------
    # Universe state
    # -----------------------------
    def get_universe_state(self) -> dict[str, Any]:
        if not self.conn:
            raise RuntimeError("DB not connected")
        cur = self.conn.cursor()
        cur.execute("SELECT symbols_csv, updated_at, next_rebuild_at, last_reason FROM universe_state WHERE id = 1")
        row = cur.fetchone()
        if not row:
            raise RuntimeError("universe_state_missing")
        symbols = [s.strip() for s in str(row["symbols_csv"] or "").split(",") if s.strip()]
        return {
            "symbols": symbols,
            "symbols_csv": str(row["symbols_csv"]),
            "updated_at": row["updated_at"],
            "next_rebuild_at": row["next_rebuild_at"],
            "last_reason": row["last_reason"],
        }

    def set_universe_state(
        self,
        symbols: list[str],
        *,
        next_rebuild_at: str | None,
        reason: str,
    ) -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")
        csv = ",".join([normalize_symbol(s) for s in symbols if str(s).strip()])
        with self.transaction():
            cur = self.conn.cursor()
            cur.execute(
                "UPDATE universe_state SET symbols_csv = ?, updated_at = ?, next_rebuild_at = ?, last_reason = ? WHERE id = 1",
                (csv, self._utc_iso(), next_rebuild_at, reason[:200]),
            )

    # -----------------------------
    # Paper balances (public)
    # -----------------------------
    def ensure_paper_asset(self, asset: str, initial_free: float = 0.0) -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")
        with self.transaction():
            cur = self.conn.cursor()
            self._ensure_paper_asset_cur(cur, asset, initial_free)

    def get_paper_balance(self, asset: str) -> float:
        if not self.conn:
            raise RuntimeError("DB not connected")
        cur = self.conn.cursor()
        return self._get_paper_balance_cur(cur, asset)

    def set_paper_balance(self, asset: str, free: float) -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")
        with self.transaction():
            cur = self.conn.cursor()
            self._set_paper_balance_cur(cur, asset, free)

    def list_paper_balances(self) -> dict[str, float]:
        if not self.conn:
            raise RuntimeError("DB not connected")
        cur = self.conn.cursor()
        cur.execute("SELECT asset, free FROM paper_balances ORDER BY asset ASC")
        rows = cur.fetchall()
        return {str(r["asset"]): float(r["free"]) for r in rows}

    def reset_paper_balances(
        self,
        usdt: float = 100.0,
        clear_positions: bool = True,
        clear_trades: bool = False,
        clear_paper_orders: bool = False,
        clear_sentiment: bool = False,
        clear_profiles: bool = False,
    ):
        if not self.conn:
            raise RuntimeError("DB not connected")
        with self.transaction():
            cur = self.conn.cursor()
            cur.execute("DELETE FROM paper_balances")
            self._ensure_paper_asset_cur(cur, "USDT", float(usdt))
            if clear_positions:
                cur.execute("DELETE FROM positions")
            if clear_trades:
                cur.execute("DELETE FROM trades")

        if clear_paper_orders:
            # Some older code/tests used a separate paper_orders table.
            # This codebase doesn't require it; ignore if absent.
            try:
                cur.execute("DELETE FROM paper_orders")
            except Exception:
                pass

        # Optional maintenance
        if clear_sentiment:
            try:
                cur.execute("DELETE FROM sentiment_snapshots")
            except Exception:
                pass
        if clear_profiles:
            try:
                cur.execute("DELETE FROM coin_profiles")
            except Exception:
                pass
        return self.list_paper_balances()

    # -----------------------------
    # Coin profiles
    # -----------------------------
    def upsert_coin_profile(self, profile: dict[str, Any]) -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")
        with self.transaction():
            cur = self.conn.cursor()
            cur.execute(
                """
                INSERT INTO coin_profiles(
                    symbol,buy_threshold,sell_threshold,min_volume_ratio,min_atr_pct,max_atr_pct,
                    downtrend_buy_penalty,uptrend_buy_boost,risk_mult,updated_at
                )
                VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                    buy_threshold=excluded.buy_threshold,
                    sell_threshold=excluded.sell_threshold,
                    min_volume_ratio=excluded.min_volume_ratio,
                    min_atr_pct=excluded.min_atr_pct,
                    max_atr_pct=excluded.max_atr_pct,
                    downtrend_buy_penalty=excluded.downtrend_buy_penalty,
                    uptrend_buy_boost=excluded.uptrend_buy_boost,
                    risk_mult=excluded.risk_mult,
                    updated_at=excluded.updated_at
                """,
                (
                    str(profile["symbol"]),
                    float(profile["buy_threshold"]),
                    float(profile["sell_threshold"]),
                    float(profile["min_volume_ratio"]),
                    float(profile["min_atr_pct"]),
                    float(profile["max_atr_pct"]),
                    float(profile["downtrend_buy_penalty"]),
                    float(profile["uptrend_buy_boost"]),
                    float(profile["risk_mult"]),
                    self._utc_iso(),
                ),
            )

    def get_coin_profile(self, symbol: str) -> dict[str, Any] | None:
        if not self.conn:
            raise RuntimeError("DB not connected")
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM coin_profiles WHERE symbol = ?", (symbol,))
        row = cur.fetchone()
        return dict(row) if row else None

    def list_coin_profiles(self) -> list[dict[str, Any]]:
        if not self.conn:
            raise RuntimeError("DB not connected")
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM coin_profiles ORDER BY symbol ASC")
        return [dict(r) for r in cur.fetchall()]

    # -----------------------------
    # Grid state persistence
    # -----------------------------
    def save_grid_state(self, symbol: str, state_dict: dict) -> None:
        """Persist grid levels for a symbol to DB. Upserts on symbol key."""
        if not self.conn:
            raise RuntimeError("DB not connected")
        import json as _json
        with self.transaction() as cur:
            cur.execute(
                """
                INSERT INTO grid_state (symbol, state_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (str(symbol), _json.dumps(state_dict, ensure_ascii=False), self._utc_iso()),
            )

    def load_grid_state(self, symbol: str) -> dict | None:
        """Load persisted grid state for a symbol. Returns None if not found."""
        if not self.conn:
            raise RuntimeError("DB not connected")
        import json as _json
        cur = self.conn.cursor()
        cur.execute(
            "SELECT state_json, updated_at FROM grid_state WHERE symbol = ?",
            (str(symbol),),
        )
        row = cur.fetchone()
        if row is None:
            return None
        try:
            data = _json.loads(row["state_json"])
            data["_db_updated_at"] = row["updated_at"]
            return data
        except Exception:
            return None

    def list_grid_states(self) -> list[dict]:
        """List all persisted grid states (for debugging/dashboard)."""
        if not self.conn:
            raise RuntimeError("DB not connected")
        import json as _json
        cur = self.conn.cursor()
        cur.execute("SELECT symbol, state_json, updated_at FROM grid_state ORDER BY symbol ASC")
        results = []
        for row in cur.fetchall():
            try:
                data = _json.loads(row["state_json"])
                data["symbol"] = row["symbol"]
                data["updated_at"] = row["updated_at"]
                results.append(data)
            except Exception:
                results.append({"symbol": row["symbol"], "updated_at": row["updated_at"], "error": "parse_failed"})
        return results

    # -----------------------------
    # Paper orders (idempotency + audit)
    # -----------------------------
    def get_paper_order_by_key(self, idempotency_key: str) -> dict[str, Any] | None:
        if not self.conn:
            raise RuntimeError("DB not connected")
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id, idempotency_key, symbol, side, amount, mid_price, exec_price, fee, slippage_bps,
                   status, reason, strategy_name, entry_reason, exit_reason, regime, atr_pct, dir_1h,
                   entry_price, exit_price, entry_cost, exit_cost, entry_fee, exit_fee, total_fees, gross_pnl, net_pnl,
                   error, created_at
            FROM paper_orders
            WHERE idempotency_key = ?
            """,
            (idempotency_key,),
        )
        r = cur.fetchone()
        if not r:
            return None

        return {
            "id": r["id"],
            "clientOrderId": r["idempotency_key"],
            "symbol": r["symbol"],
            "side": r["side"].lower(),
            "type": "market",
            "amount": float(r["amount"]),
            "filled": float(r["amount"]) if r["status"] == "FILLED" else 0.0,
            "price": float(r["exec_price"]),
            "average": float(r["exec_price"]),
            "cost": float(r["amount"]) * float(r["exec_price"]),
            "fee": float(r["fee"]),
            "status": "closed" if r["status"] == "FILLED" else "rejected",
            "timestamp": int(datetime.fromisoformat(r["created_at"]).timestamp() * 1000),
            "info": {
                "mode": "paper",
                "mid_price": float(r["mid_price"]),
                "slippage_bps": float(r["slippage_bps"]),
                "reason": r["reason"],
                "strategy_name": r["strategy_name"],
                "entry_reason": r["entry_reason"],
                "exit_reason": r["exit_reason"],
                "regime": r["regime"],
                "atr_pct": float(r["atr_pct"]) if r["atr_pct"] is not None else None,
                "dir_1h": r["dir_1h"],
                "entry_price": float(r["entry_price"]) if r["entry_price"] is not None else None,
                "exit_price": float(r["exit_price"]) if r["exit_price"] is not None else None,
                "entry_cost": float(r["entry_cost"]) if r["entry_cost"] is not None else None,
                "exit_cost": float(r["exit_cost"]) if r["exit_cost"] is not None else None,
                "entry_fee": float(r["entry_fee"]) if r["entry_fee"] is not None else None,
                "exit_fee": float(r["exit_fee"]) if r["exit_fee"] is not None else None,
                "total_fees": float(r["total_fees"]) if r["total_fees"] is not None else None,
                "gross_pnl": float(r["gross_pnl"]) if r["gross_pnl"] is not None else None,
                "net_pnl": float(r["net_pnl"]) if r["net_pnl"] is not None else None,
                "status": r["status"],
                "error": r["error"],
            },
        }

    def create_paper_order(
        self,
        idempotency_key: str,
        symbol: str,
        side: str,
        amount: float,
        mid_price: float,
        exec_price: float,
        fee: float,
        slippage_bps: float,
        reason: str,
        strategy_name: str | None = None,
        entry_reason: str | None = None,
        exit_reason: str | None = None,
        regime: str | None = None,
        atr_pct: float | None = None,
        dir_1h: str | None = None,
        entry_price: float | None = None,
        exit_price: float | None = None,
        entry_cost: float | None = None,
        exit_cost: float | None = None,
        entry_fee: float | None = None,
        exit_fee: float | None = None,
        total_fees: float | None = None,
        gross_pnl: float | None = None,
        net_pnl: float | None = None,
    ) -> str:
        if not self.conn:
            raise RuntimeError("DB not connected")

        order_id = "po_" + idempotency_key[:24]
        now = self._utc_iso()
        with self.transaction():
            cur = self.conn.cursor()
            cur.execute(
                """
                INSERT INTO paper_orders (
                    id, idempotency_key, created_at, updated_at,
                    symbol, side, amount, mid_price, exec_price, fee, slippage_bps,
                    status, reason, strategy_name, entry_reason, exit_reason, regime, atr_pct, dir_1h,
                    entry_price, exit_price, entry_cost, exit_cost, entry_fee, exit_fee, total_fees, gross_pnl, net_pnl, error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    idempotency_key,
                    now,
                    now,
                    symbol,
                    side.upper().strip(),
                    float(amount),
                    float(mid_price),
                    float(exec_price),
                    float(fee),
                    float(slippage_bps),
                    "NEW",
                    reason,
                    strategy_name,
                    entry_reason,
                    exit_reason,
                    regime,
                    None if atr_pct is None else float(atr_pct),
                    dir_1h,
                    None if entry_price is None else float(entry_price),
                    None if exit_price is None else float(exit_price),
                    None if entry_cost is None else float(entry_cost),
                    None if exit_cost is None else float(exit_cost),
                    None if entry_fee is None else float(entry_fee),
                    None if exit_fee is None else float(exit_fee),
                    None if total_fees is None else float(total_fees),
                    None if gross_pnl is None else float(gross_pnl),
                    None if net_pnl is None else float(net_pnl),
                    None,
                ),
            )
        return order_id

    def update_paper_order_audit(
        self,
        order_id: str,
        *,
        strategy_name: str | None = None,
        entry_reason: str | None = None,
        exit_reason: str | None = None,
        regime: str | None = None,
        atr_pct: float | None = None,
        dir_1h: str | None = None,
        entry_price: float | None = None,
        exit_price: float | None = None,
        entry_cost: float | None = None,
        exit_cost: float | None = None,
        entry_fee: float | None = None,
        exit_fee: float | None = None,
        total_fees: float | None = None,
        gross_pnl: float | None = None,
        net_pnl: float | None = None,
    ) -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")

        fields: dict[str, Any] = {
            "strategy_name": strategy_name,
            "entry_reason": entry_reason,
            "exit_reason": exit_reason,
            "regime": regime,
            "atr_pct": atr_pct,
            "dir_1h": dir_1h,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "entry_cost": entry_cost,
            "exit_cost": exit_cost,
            "entry_fee": entry_fee,
            "exit_fee": exit_fee,
            "total_fees": total_fees,
            "gross_pnl": gross_pnl,
            "net_pnl": net_pnl,
        }
        assignments: list[str] = []
        values: list[Any] = []
        for key, value in fields.items():
            if value is None:
                continue
            assignments.append(f"{key} = ?")
            values.append(float(value) if key in {"atr_pct", "entry_price", "exit_price", "entry_cost", "exit_cost", "entry_fee", "exit_fee", "total_fees", "gross_pnl", "net_pnl"} else value)
        if not assignments:
            return

        assignments.append("updated_at = ?")
        values.append(self._utc_iso())
        values.append(order_id)

        with self.transaction():
            cur = self.conn.cursor()
            cur.execute(
                f"UPDATE paper_orders SET {', '.join(assignments)} WHERE id = ?",
                tuple(values),
            )

    def set_paper_order_status(self, order_id: str, status: str, error: str | None) -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")
        with self.transaction():
            cur = self.conn.cursor()
            cur.execute(
                "UPDATE paper_orders SET status = ?, error = ?, updated_at = ? WHERE id = ?",
                (status.upper().strip(), error, self._utc_iso(), order_id),
            )

    def apply_paper_wallet_delta(
        self,
        symbol: str,
        side: str,
        amount: float,
        exec_price: float,
        fee: float,
    ) -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")

        base, quote = symbol.split("/")
        base = base.upper().strip()
        quote = quote.upper().strip()
        side_u = side.upper().strip()

        amount_f = float(amount)
        price_f = float(exec_price)
        fee_f = float(fee)

        if amount_f <= 0 or price_f <= 0:
            raise ValueError("amount/exec_price must be > 0")
        if fee_f < 0:
            raise ValueError("fee must be >= 0")

        cost = amount_f * price_f

        with self.transaction():
            cur = self.conn.cursor()
            self._ensure_paper_asset_cur(cur, base, 0.0)
            self._ensure_paper_asset_cur(cur, quote, 0.0)

            qb = self._get_paper_balance_cur(cur, quote)
            bb = self._get_paper_balance_cur(cur, base)

            if side_u == "BUY":
                need = cost + fee_f
                if qb + 1e-12 < need:
                    raise RuntimeError(f"PAPER yetersiz {quote}: have={qb:.6f} need={need:.6f}")
                self._set_paper_balance_cur(cur, quote, qb - need)
                self._set_paper_balance_cur(cur, base, bb + amount_f)
            elif side_u == "SELL":
                if bb + 1e-12 < amount_f:
                    # Paper SELL: balance yetersiz ama pozisyon kapatma kritik
                    # Grid seviyeleri ve DB balance sync olmayabilir
                    # Mevcut balance'ı kullan, sıfırla ve devam et
                    actual_sell = max(bb, 0.0)
                    logger.warning(
                        "paper_sell_balance_clip sym=%s have=%.8f need=%.8f → selling=%.8f",
                        symbol if "symbol" in dir() else base, bb, amount_f, actual_sell
                    )
                    self._set_paper_balance_cur(cur, base, 0.0)
                    self._set_paper_balance_cur(cur, quote, qb + (actual_sell * float(price)) - fee_f)
                else:
                    self._set_paper_balance_cur(cur, base, bb - amount_f)
                    self._set_paper_balance_cur(cur, quote, qb + cost - fee_f)
            else:
                raise ValueError("side must be BUY or SELL")

    # -----------------------------
    # Positions
    # -----------------------------
    def list_open_positions(self) -> list[dict[str, Any]]:
        if not self.conn:
            raise RuntimeError("DB not connected")

        cur = self.conn.cursor()
        cur.execute("PRAGMA table_info(positions)")
        columns = {str(row["name"]) for row in cur.fetchall()}
        if not columns:
            return []

        # Support both the current schema and older status-based fixtures.
        if {"id", "side", "status"}.issubset(columns):
            cur.execute(
                """
                SELECT
                    id,
                    symbol,
                    side,
                    entry_price,
                    amount,
                    stop_loss,
                    take_profit,
                    opened_at,
                    updated_at,
                    status
                FROM positions
                WHERE status = 'OPEN'
                ORDER BY COALESCE(opened_at, updated_at) DESC
                """
            )
            return [dict(row) for row in cur.fetchall()]

        return self.get_open_positions()

    def get_open_positions(self) -> list[dict[str, Any]]:
        """Thread-safe: kendi bağlantısını açar, row_factory garantili."""
        import sqlite3 as _sqlite3
        try:
            conn = _sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.row_factory = _sqlite3.Row
            cur = conn.cursor()
            cur.execute("""
                SELECT symbol, amount, entry_price, cost, updated_at,
                       stop_loss, take_profit, highest_price, opened_at,
                       strategy_name, entry_reason, regime, atr_pct, dir_1h, entry_fee_total
                FROM positions
            """)
            rows = cur.fetchall()
            result = []
            for r in rows:
                try:
                    result.append({
                        "symbol": r["symbol"],
                        "amount": float(r["amount"] or 0.0),
                        "entry_price": float(r["entry_price"] or 0.0),
                        "cost": float(r["cost"] or 0.0),
                        "updated_at": r["updated_at"],
                        "stop_loss": float(r["stop_loss"]) if r["stop_loss"] is not None else None,
                        "take_profit": float(r["take_profit"]) if r["take_profit"] is not None else None,
                        "highest_price": float(r["highest_price"]) if r["highest_price"] is not None else None,
                        "opened_at": r["opened_at"],
                        "strategy_name": r["strategy_name"],
                        "entry_reason": r["entry_reason"],
                        "regime": r["regime"],
                        "atr_pct": float(r["atr_pct"]) if r["atr_pct"] is not None else None,
                        "dir_1h": r["dir_1h"],
                        "entry_fee_total": float(r["entry_fee_total"]) if r["entry_fee_total"] is not None else 0.0,
                    })
                except Exception:
                    pass  # bozuk satırı atla
            conn.close()
            return result
        except Exception as e:
            logger.error("get_open_positions error: %s", e)
            return []

    def get_open_position(self, symbol: str) -> dict[str, Any] | None:
        if not self.conn:
            raise RuntimeError("DB not connected")
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT symbol, amount, entry_price, cost, updated_at,
                   stop_loss, take_profit, highest_price, opened_at,
                   strategy_name, entry_reason, regime, atr_pct, dir_1h, entry_fee_total
            FROM positions
            WHERE symbol = ?
            """,
            (symbol,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return {
            "symbol": r["symbol"],
            "amount": float(r["amount"]),
            "entry_price": float(r["entry_price"]),
            "cost": float(r["cost"]),
            "updated_at": r["updated_at"],
            "stop_loss": float(r["stop_loss"]) if r["stop_loss"] is not None else None,
            "take_profit": float(r["take_profit"]) if r["take_profit"] is not None else None,
            "highest_price": float(r["highest_price"]) if r["highest_price"] is not None else None,
            "opened_at": r["opened_at"],
            "strategy_name": r["strategy_name"],
            "entry_reason": r["entry_reason"],
            "regime": r["regime"],
            "atr_pct": float(r["atr_pct"]) if r["atr_pct"] is not None else None,
            "dir_1h": r["dir_1h"],
            "entry_fee_total": float(r["entry_fee_total"]) if r["entry_fee_total"] is not None else 0.0,
        }

    def update_position_audit(self, symbol: str, side: str, amount: float, price: float, fee: float = 0.0) -> dict[str, Any]:
        if not self.conn:
            raise RuntimeError("DB not connected")

        side_u = side.upper().strip()
        now = self._utc_iso()
        amount_f = float(amount)
        price_f = float(price)
        fee_f = float(fee)

        def _empty_audit() -> dict[str, Any]:
            return {
                "symbol": symbol,
                "side": side_u,
                "amount": amount_f,
                "entry_price": None,
                "exit_price": None,
                "entry_cost": None,
                "exit_cost": None,
                "entry_fee": None,
                "exit_fee": None,
                "total_fees": None,
                "gross_pnl": None,
                "net_pnl": None,
                "realized_pnl": 0.0,
                "strategy_name": None,
                "entry_reason": None,
                "regime": None,
                "atr_pct": None,
                "dir_1h": None,
            }

        with self.transaction():
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT symbol, amount, entry_price, cost, highest_price, opened_at,
                       strategy_name, entry_reason, regime, atr_pct, dir_1h, entry_fee_total
                FROM positions
                WHERE symbol = ?
                """,
                (symbol,),
            )
            row = cur.fetchone()

            if side_u == "BUY":
                cost_add = amount_f * price_f
                if row:
                    new_amount = float(row["amount"]) + amount_f
                    new_cost = float(row["cost"]) + float(cost_add)
                    new_entry = new_cost / new_amount if new_amount > 0 else 0.0
                    new_entry_fee_total = float(row["entry_fee_total"] or 0.0) + fee_f

                    cur.execute(
                        "UPDATE positions SET amount=?, entry_price=?, cost=?, entry_fee_total=?, updated_at=? WHERE symbol=?",
                        (new_amount, new_entry, new_cost, new_entry_fee_total, now, symbol),
                    )

                    hp = row["highest_price"]
                    if hp is None or price_f > float(hp):
                        cur.execute("UPDATE positions SET highest_price = ? WHERE symbol = ?", (price_f, symbol))

                    if row["opened_at"] is None:
                        cur.execute("UPDATE positions SET opened_at = ? WHERE symbol = ?", (now, symbol))
                else:
                    cur.execute(
                        """
                        INSERT INTO positions (
                            symbol, amount, entry_price, cost, updated_at,
                            stop_loss, take_profit, highest_price, opened_at, entry_fee_total
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            symbol,
                            amount_f,
                            price_f,
                            float(cost_add),
                            now,
                            None,
                            None,
                            price_f,
                            now,
                            fee_f,
                        ),
                    )
                return {
                    "symbol": symbol,
                    "side": side_u,
                    "amount": amount_f,
                    "entry_price": price_f,
                    "exit_price": None,
                    "entry_cost": float(cost_add),
                    "exit_cost": None,
                    "entry_fee": fee_f,
                    "exit_fee": None,
                    "total_fees": fee_f,
                    "gross_pnl": None,
                    "net_pnl": None,
                    "realized_pnl": 0.0,
                    "strategy_name": row["strategy_name"] if row else None,
                    "entry_reason": row["entry_reason"] if row else None,
                    "regime": row["regime"] if row else None,
                    "atr_pct": float(row["atr_pct"]) if row and row["atr_pct"] is not None else None,
                    "dir_1h": row["dir_1h"] if row else None,
                }

            if side_u == "SELL":
                if not row:
                    return _empty_audit()

                pos_amount = float(row["amount"])
                pos_cost = float(row["cost"])
                pos_entry_fee_total = float(row["entry_fee_total"] or 0.0)

                sell_amount = min(amount_f, pos_amount)
                if sell_amount <= 0:
                    return _empty_audit()

                allocation_ratio = sell_amount / pos_amount if pos_amount > 0 else 0.0
                entry_cost = pos_cost * allocation_ratio
                entry_fee = pos_entry_fee_total * allocation_ratio
                exit_cost = sell_amount * price_f
                gross_pnl = exit_cost - entry_cost
                total_fees = entry_fee + fee_f
                net_pnl = gross_pnl - total_fees
                realized_pnl = net_pnl
                entry_price = entry_cost / sell_amount if sell_amount > 0 else None

                remaining = pos_amount - sell_amount
                if remaining <= 1e-12:
                    cur.execute("DELETE FROM positions WHERE symbol = ?", (symbol,))
                else:
                    remaining_cost = max(0.0, pos_cost - entry_cost)
                    new_entry = remaining_cost / remaining if remaining > 0 else 0.0
                    remaining_entry_fee_total = max(0.0, pos_entry_fee_total - entry_fee)
                    cur.execute(
                        "UPDATE positions SET amount=?, entry_price=?, cost=?, entry_fee_total=?, updated_at=? WHERE symbol=?",
                        (remaining, new_entry, remaining_cost, remaining_entry_fee_total, now, symbol),
                    )

                return {
                    "symbol": symbol,
                    "side": side_u,
                    "amount": sell_amount,
                    "entry_price": float(entry_price) if entry_price is not None else None,
                    "exit_price": price_f,
                    "entry_cost": float(entry_cost),
                    "exit_cost": float(exit_cost),
                    "entry_fee": float(entry_fee),
                    "exit_fee": fee_f,
                    "total_fees": float(total_fees),
                    "gross_pnl": float(gross_pnl),
                    "net_pnl": float(net_pnl),
                    "realized_pnl": float(realized_pnl),
                    "strategy_name": row["strategy_name"],
                    "entry_reason": row["entry_reason"],
                    "regime": row["regime"],
                    "atr_pct": float(row["atr_pct"]) if row["atr_pct"] is not None else None,
                    "dir_1h": row["dir_1h"],
                }

            return _empty_audit()

    def update_position(self, symbol: str, side: str, amount: float, price: float, fee: float = 0.0) -> float:
        audit = self.update_position_audit(symbol=symbol, side=side, amount=amount, price=price, fee=fee)
        return float(audit.get("realized_pnl") or 0.0)

    def set_position_risk(self, symbol: str, stop_loss: float | None, take_profit: float | None) -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")
        with self.transaction():
            cur = self.conn.cursor()
            cur.execute(
                "UPDATE positions SET stop_loss = ?, take_profit = ?, updated_at = ? WHERE symbol = ?",
                (stop_loss, take_profit, self._utc_iso(), symbol),
            )

    def set_position_signal_meta(
        self,
        symbol: str,
        *,
        strategy_name: str | None = None,
        entry_reason: str | None = None,
        regime: str | None = None,
        atr_pct: float | None = None,
        dir_1h: str | None = None,
    ) -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")
        with self.transaction():
            cur = self.conn.cursor()
            cur.execute(
                """
                UPDATE positions
                SET strategy_name = ?, entry_reason = ?, regime = ?, atr_pct = ?, dir_1h = ?, updated_at = ?
                WHERE symbol = ?
                """,
                (
                    strategy_name,
                    entry_reason,
                    regime,
                    None if atr_pct is None else float(atr_pct),
                    dir_1h,
                    self._utc_iso(),
                    symbol,
                ),
            )

    def update_highest_price(self, symbol: str, highest_price: float) -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")
        with self.transaction():
            cur = self.conn.cursor()
            cur.execute(
                "UPDATE positions SET highest_price = ?, updated_at = ? WHERE symbol = ?",
                (float(highest_price), self._utc_iso(), symbol),
            )

    def get_today_realized_pnl(self) -> float:
        if not self.conn:
            raise RuntimeError("DB not connected")

        table = "trades"
        pnl_col_candidates = ["net_pnl", "realized_pnl", "pnl"]
        ts_col = "timestamp"

        cur = self.conn.cursor()
        # trades tablosunun gerçek kolon listesini al, timestamp yoksa alternatif dene
        try:
            _all_cols_rows = cur.execute("PRAGMA table_info(trades)").fetchall()
            _all_cols = set()
            for r in _all_cols_rows:
                try: _all_cols.add(str(r["name"]))
                except Exception:
                    if len(r) > 1: _all_cols.add(str(r[1]))
            for _candidate_ts in ["timestamp", "created_at", "time", "ts"]:
                if _candidate_ts in _all_cols:
                    ts_col = _candidate_ts
                    break
        except Exception:
            pass

        def _table_exists(table_name: str) -> bool:
            row = cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            ).fetchone()
            return row is not None

        def _columns(table_name: str) -> set[str]:
            rows = cur.execute(f"PRAGMA table_info({table_name})").fetchall()
            result = set()
            for row in rows:
                try:
                    result.add(str(row["name"]))
                except (IndexError, TypeError):
                    if len(row) > 1:
                        result.add(str(row[1]))
            return result

        if not _table_exists(table):
            logger.warning("get_today_realized_pnl: table '%s' not found", table)
            return 0.0

        cols = _columns(table)
        if ts_col not in cols:
            logger.warning("get_today_realized_pnl: '%s.%s' not found", table, ts_col)
            return 0.0

        pnl_expr = self._effective_trade_pnl_expr(cols)
        if pnl_expr is None:
            logger.warning(
                "get_today_realized_pnl: pnl col not found in %s (tried %s)",
                table,
                pnl_col_candidates,
            )
            return 0.0

        sample_row = cur.execute(
            f"""
            SELECT {ts_col} AS ts
            FROM {table}
            WHERE {ts_col} IS NOT NULL AND TRIM({ts_col}) <> ''
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        sample_ts = str(sample_row["ts"]) if sample_row and sample_row["ts"] is not None else None
        dr = _tr_today_range_to_utc_naive_iso_keys(sample_ts=sample_ts)

        if sample_ts:
            logger.debug(
                "get_today_realized_pnl format sample=%s start_key=%s end_key=%s",
                sample_ts,
                dr.start_key,
                dr.end_key,
            )

        row = cur.execute(
            f"""
            SELECT COALESCE(SUM({pnl_expr}), 0.0) AS s
            FROM {table}
            WHERE {ts_col} >= ? AND {ts_col} < ?
            """,
            (dr.start_key, dr.end_key),
        ).fetchone()

        return float(row["s"] or 0.0)

    # -----------------------------
    # Trades
    # -----------------------------
    def add_trade(
        self,
        *,
        timestamp: str | None = None,
        symbol: str,
        side: str,
        amount: float,
        price: float,
        cost: float,
        fee: float = 0.0,
        realized_pnl: float | None = 0.0,
        strategy_name: str | None = None,
        entry_reason: str | None = None,
        exit_reason: str | None = None,
        regime: str | None = None,
        atr_pct: float | None = None,
        dir_1h: str | None = None,
        entry_price: float | None = None,
        exit_price: float | None = None,
        entry_cost: float | None = None,
        exit_cost: float | None = None,
        entry_fee: float | None = None,
        exit_fee: float | None = None,
        total_fees: float | None = None,
        gross_pnl: float | None = None,
        net_pnl: float | None = None,
    ) -> int:
        # trades.timestamp must be UTC-naive ISO string (TEXT) for TR-day boundary queries.
        ts = (timestamp or utc_naive_iso_now(timespec="seconds")).strip()
        sym = str(symbol).strip()
        side_u = str(side).strip().upper()

        if not sym:
            raise ValueError("add_trade: symbol required")
        if side_u not in ("BUY", "SELL"):
            raise ValueError(f"add_trade: invalid side={side}")
        if float(amount) <= 0 or float(price) <= 0 or float(cost) < 0:
            raise ValueError("add_trade: invalid numeric values")

        effective_realized_pnl = float(net_pnl) if net_pnl is not None else float(realized_pnl or 0.0)

        # Use primary connection + transaction lock. Avoid split-brain between multiple SQLite connections.
        with self.transaction() as cur:
            cur.execute(
                """
                INSERT INTO trades (
                    timestamp, symbol, side, amount, price, cost, fee, realized_pnl,
                    strategy_name, entry_reason, exit_reason, regime, atr_pct, dir_1h,
                    entry_price, exit_price, entry_cost, exit_cost, entry_fee, exit_fee, total_fees, gross_pnl, net_pnl
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    sym,
                    side_u,
                    float(amount),
                    float(price),
                    float(cost),
                    float(fee),
                    effective_realized_pnl,
                    strategy_name,
                    entry_reason,
                    exit_reason,
                    regime,
                    None if atr_pct is None else float(atr_pct),
                    dir_1h,
                    None if entry_price is None else float(entry_price),
                    None if exit_price is None else float(exit_price),
                    None if entry_cost is None else float(entry_cost),
                    None if exit_cost is None else float(exit_cost),
                    None if entry_fee is None else float(entry_fee),
                    None if exit_fee is None else float(exit_fee),
                    None if total_fees is None else float(total_fees),
                    None if gross_pnl is None else float(gross_pnl),
                    None if net_pnl is None else float(net_pnl),
                ),
            )
            return int(cur.lastrowid)

    def insert_trade(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float,
        fee: float = 0.0,
        realized_pnl: float | None = 0.0,
        strategy_name: str | None = None,
        entry_reason: str | None = None,
        exit_reason: str | None = None,
        regime: str | None = None,
        atr_pct: float | None = None,
        dir_1h: str | None = None,
        entry_price: float | None = None,
        exit_price: float | None = None,
        entry_cost: float | None = None,
        exit_cost: float | None = None,
        entry_fee: float | None = None,
        exit_fee: float | None = None,
        total_fees: float | None = None,
        gross_pnl: float | None = None,
        net_pnl: float | None = None,
    ) -> None:
        self.add_trade(
            timestamp=self._utc_iso(),
            symbol=symbol,
            side=side,
            amount=float(amount),
            price=float(price),
            cost=float(amount) * float(price),
            fee=float(fee),
            realized_pnl=None if realized_pnl is None else float(realized_pnl),
            strategy_name=strategy_name,
            entry_reason=entry_reason,
            exit_reason=exit_reason,
            regime=regime,
            atr_pct=atr_pct,
            dir_1h=dir_1h,
            entry_price=entry_price,
            exit_price=exit_price,
            entry_cost=entry_cost,
            exit_cost=exit_cost,
            entry_fee=entry_fee,
            exit_fee=exit_fee,
            total_fees=total_fees,
            gross_pnl=gross_pnl,
            net_pnl=net_pnl,
        )

    def add_signal_audit(
        self,
        *,
        timestamp: str | None = None,
        symbol: str,
        strategy_name: str | None = None,
        decision: str,
        signal: str,
        score: float = 0.0,
        buy_threshold: float | None = None,
        gap_to_threshold: float | None = None,
        regime: str | None = None,
        dir_1h: str | None = None,
        atr_pct: float | None = None,
        price: float | None = None,
        blocked_reason: str | None = None,
        trade_blocked: bool = False,
        exec_blocked: bool = False,
        gate_status: dict[str, Any] | None = None,
        hold_fail_reasons: list[str] | None = None,
        reason: str | None = None,
        risk_multiplier: float | None = None,
        corr_factor: float | None = None,
        corr_reason: str | None = None,
    ) -> int:
        if not self.conn:
            raise RuntimeError("DB not connected")

        ts = (timestamp or utc_naive_iso_now(timespec="seconds")).strip()
        sym = str(symbol).strip()
        decision_text = str(decision).strip()
        signal_text = str(signal).strip().upper()
        if not sym:
            raise ValueError("add_signal_audit: symbol required")
        if not decision_text:
            raise ValueError("add_signal_audit: decision required")
        if not signal_text:
            raise ValueError("add_signal_audit: signal required")

        with self.transaction() as cur:
            cur.execute(
                """
                INSERT INTO signal_audit (
                    timestamp, symbol, strategy_name, decision, signal, score, buy_threshold, gap_to_threshold,
                    regime, dir_1h, atr_pct, price, blocked_reason, trade_blocked, exec_blocked,
                    gate_status_json, hold_fail_reasons_json, reason, risk_multiplier, corr_factor, corr_reason
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    sym,
                    strategy_name,
                    decision_text,
                    signal_text,
                    float(score),
                    None if buy_threshold is None else float(buy_threshold),
                    None if gap_to_threshold is None else float(gap_to_threshold),
                    regime,
                    dir_1h,
                    None if atr_pct is None else float(atr_pct),
                    None if price is None else float(price),
                    blocked_reason,
                    1 if trade_blocked else 0,
                    1 if exec_blocked else 0,
                    self._json_dumps(gate_status),
                    self._json_dumps(hold_fail_reasons or []),
                    reason,
                    None if risk_multiplier is None else float(risk_multiplier),
                    None if corr_factor is None else float(corr_factor),
                    corr_reason,
                ),
            )
            return int(cur.lastrowid)

    def add_signal_audits_batch(self, audits: list[dict]) -> None:
        """Sinyal loglarını DB'ye toplu (batch) olarak yazar."""
        if not audits:
            return
        
        try:
            self.connect()
            
            # Prepare batch data
            records = []
            for audit in audits:
                records.append((
                    audit.get("symbol"),
                    audit.get("strategy_name"),
                    audit.get("decision"),
                    audit.get("signal"),
                    audit.get("score"),
                    audit.get("buy_threshold"),
                    audit.get("gap_to_threshold"),
                    audit.get("regime"),
                    audit.get("dir_1h"),
                    audit.get("atr_pct"),
                    audit.get("price"),
                    audit.get("blocked_reason"),
                    audit.get("trade_blocked", False),
                    audit.get("exec_blocked", False),
                    json.dumps(audit.get("gate_status")) if audit.get("gate_status") else None,
                    json.dumps(audit.get("hold_fail_reasons")) if audit.get("hold_fail_reasons") else None,
                    audit.get("reason"),
                    audit.get("risk_multiplier"),
                    audit.get("corr_factor"),
                    audit.get("corr_reason"),
                ))
            
            # Single transaction for entire batch
            self.conn.executemany(
                """
                INSERT INTO signal_audit (
                    symbol, strategy_name, decision, signal, score,
                    buy_threshold, gap_to_threshold, regime, dir_1h, atr_pct,
                    price, blocked_reason, trade_blocked, exec_blocked,
                    gate_status, hold_fail_reasons, reason, risk_multiplier,
                    corr_factor, corr_reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                records
            )
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Batch signal audit error: {e}")
            if self.conn:
                self.conn.rollback()

    # -----------------------------
    # Sentiment snapshots
    # -----------------------------
    def upsert_sentiment_snapshot(
        self,
        symbol: str,
        ts: int,
        score: float,
        analyzed: int = 0,
        important: int = 0,
        pos_votes: int = 0,
        neg_votes: int = 0,
        source: str = "cryptopanic",
    ) -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")

        symbol_u = symbol.split("/")[0].upper().strip()

        with self.transaction():
            cur = self.conn.cursor()
            cur.execute(
                "SELECT id FROM sentiment_snapshots WHERE symbol = ? AND ts = ? AND source = ?",
                (symbol_u, int(ts), source),
            )
            row = cur.fetchone()

            if row:
                cur.execute(
                    """
                    UPDATE sentiment_snapshots
                    SET score = ?, analyzed = ?, important = ?, pos_votes = ?, neg_votes = ?
                    WHERE symbol = ? AND ts = ? AND source = ?
                    """,
                    (
                        float(score),
                        int(analyzed),
                        int(important),
                        int(pos_votes),
                        int(neg_votes),
                        symbol_u,
                        int(ts),
                        source,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO sentiment_snapshots (ts, symbol, score, analyzed, important, pos_votes, neg_votes, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (int(ts), symbol_u, float(score), int(analyzed), int(important), int(pos_votes), int(neg_votes), source),
                )

    def prune_sentiment_snapshots(self, keep_days: int = 60, source: str = "cryptopanic") -> None:
        if not self.conn:
            raise RuntimeError("DB not connected")
        cutoff = int((datetime.utcnow().timestamp()) - int(keep_days) * 86400)
        with self.transaction():
            cur = self.conn.cursor()
            cur.execute("DELETE FROM sentiment_snapshots WHERE ts < ? AND source = ?", (cutoff, source))

    # -----------------------------
    # Dashboard
    # -----------------------------
    def get_dashboard_trades(self, limit: int = 50) -> list[dict[str, Any]]:
        import sqlite3 as _sqlite3
        _conn = _sqlite3.connect(str(self.db_path), check_same_thread=False)
        _conn.row_factory = _sqlite3.Row
        cur = _conn.cursor()
        cur.execute(
            """
            SELECT timestamp, symbol, side, amount, price, cost, fee, realized_pnl,
                   strategy_name, entry_reason, exit_reason, regime, atr_pct, dir_1h,
                   entry_price, exit_price, entry_cost, exit_cost, entry_fee, exit_fee, total_fees, gross_pnl, net_pnl
            FROM trades
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = cur.fetchall()
        _conn.close()
        return [
            {
                "timestamp": r["timestamp"],
                "symbol": r["symbol"],
                "side": r["side"],
                "amount": float(r["amount"]),
                "price": float(r["price"]),
                "cost": float(r["cost"]),
                "fee": float(r["fee"]),
                "realized_pnl": float(r["realized_pnl"]),
                "strategy_name": r["strategy_name"],
                "entry_reason": r["entry_reason"],
                "exit_reason": r["exit_reason"],
                "regime": r["regime"],
                "atr_pct": float(r["atr_pct"]) if r["atr_pct"] is not None else None,
                "dir_1h": r["dir_1h"],
                "entry_price": float(r["entry_price"]) if r["entry_price"] is not None else None,
                "exit_price": float(r["exit_price"]) if r["exit_price"] is not None else None,
                "entry_cost": float(r["entry_cost"]) if r["entry_cost"] is not None else None,
                "exit_cost": float(r["exit_cost"]) if r["exit_cost"] is not None else None,
                "entry_fee": float(r["entry_fee"]) if r["entry_fee"] is not None else None,
                "exit_fee": float(r["exit_fee"]) if r["exit_fee"] is not None else None,
                "total_fees": float(r["total_fees"]) if r["total_fees"] is not None else None,
                "gross_pnl": float(r["gross_pnl"]) if r["gross_pnl"] is not None else None,
                "net_pnl": float(r["net_pnl"]) if r["net_pnl"] is not None else None,
            }
            for r in rows
        ]

    def get_closed_trade_exports(self, limit: int | None = 50) -> list[dict[str, Any]]:
        if not self.conn:
            raise RuntimeError("DB not connected")

        cur = self.conn.cursor()
        sql = """
            SELECT
                id, timestamp, symbol, strategy_name, entry_reason, exit_reason, regime, dir_1h,
                entry_price, exit_price, entry_cost, exit_cost, entry_fee, exit_fee, total_fees, gross_pnl, net_pnl, realized_pnl
            FROM trades
            WHERE side = 'SELL'
            ORDER BY id DESC
        """
        params: tuple[Any, ...] = ()
        if limit is not None:
            sql += " LIMIT ?"
            params = (int(limit),)
        cur.execute(sql, params)
        rows = cur.fetchall()
        exports: list[dict[str, Any]] = []
        for row in rows:
            exports.append(
                {
                    "id": int(row["id"]),
                    "timestamp": row["timestamp"],
                    "symbol": row["symbol"],
                    "strategy_name": row["strategy_name"],
                    "entry_reason": row["entry_reason"],
                    "exit_reason": row["exit_reason"],
                    "regime": row["regime"],
                    "dir_1h": row["dir_1h"],
                    "entry_price": float(row["entry_price"]) if row["entry_price"] is not None else None,
                    "exit_price": float(row["exit_price"]) if row["exit_price"] is not None else None,
                    "entry_cost": float(row["entry_cost"]) if row["entry_cost"] is not None else None,
                    "exit_cost": float(row["exit_cost"]) if row["exit_cost"] is not None else None,
                    "entry_fee": float(row["entry_fee"]) if row["entry_fee"] is not None else None,
                    "exit_fee": float(row["exit_fee"]) if row["exit_fee"] is not None else None,
                    "total_fees": float(row["total_fees"]) if row["total_fees"] is not None else None,
                    "gross_pnl": float(row["gross_pnl"]) if row["gross_pnl"] is not None else None,
                    "net_pnl": float(self._effective_trade_pnl_value(row) or 0.0),
                }
            )
        return exports

    def get_closed_trade_analytics(self) -> dict[str, Any]:
        if not self.conn:
            raise RuntimeError("DB not connected")

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT
                timestamp, symbol, side, amount, price, cost, fee, realized_pnl, net_pnl,
                strategy_name, entry_reason, exit_reason, regime, atr_pct, dir_1h,
                entry_price, exit_price, entry_cost, exit_cost, entry_fee, exit_fee, total_fees, gross_pnl
            FROM trades
            WHERE side = 'SELL'
            ORDER BY timestamp ASC, id ASC
            """
        )
        raw_rows = cur.fetchall()

        def _build_group_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
            pnls = [float(item["effective_net_pnl"]) for item in items]
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p < 0]
            gross_profit_sum = float(sum(wins))
            gross_loss_sum = float(abs(sum(losses)))
            fee_sum = float(sum(float(item["effective_fee_sum"]) for item in items))
            net_pnl_sum = float(sum(pnls))
            return {
                "total_closed_trades": len(items),
                "wins": len(wins),
                "losses": len(losses),
                "win_rate": float(len(wins) / len(items)) if items else 0.0,
                "gross_profit_sum": gross_profit_sum,
                "gross_loss_sum": gross_loss_sum,
                "fee_sum": fee_sum,
                "net_pnl_sum": net_pnl_sum,
                "avg_win_net_pnl": float(gross_profit_sum / len(wins)) if wins else 0.0,
                "avg_loss_net_pnl": float(sum(losses) / len(losses)) if losses else 0.0,
                "expectancy_net_pnl": float(net_pnl_sum / len(items)) if items else 0.0,
                "profit_factor_net": self._json_safe_profit_factor(gross_profit_sum, gross_loss_sum),
            }

        closed_rows: list[dict[str, Any]] = []
        for row in raw_rows:
            effective_net = self._effective_trade_pnl_value(row)
            if effective_net is None:
                continue
            effective_fee_sum = row["total_fees"]
            if effective_fee_sum is None:
                fee_parts = [row["entry_fee"], row["exit_fee"]]
                if any(part is not None for part in fee_parts):
                    effective_fee_sum = sum(self._safe_float(part, 0.0) for part in fee_parts if part is not None)
                else:
                    effective_fee_sum = self._safe_float(row["fee"], 0.0)
            closed_rows.append(
                {
                    "timestamp": row["timestamp"],
                    "symbol": row["symbol"],
                    "exit_reason": row["exit_reason"] or "UNKNOWN",
                    "strategy_name": row["strategy_name"] or "UNKNOWN",
                    "effective_net_pnl": float(effective_net),
                    "effective_fee_sum": float(effective_fee_sum or 0.0),
                }
            )

        summary = _build_group_summary(closed_rows)

        by_strategy: dict[str, list[dict[str, Any]]] = {}
        by_symbol: dict[str, list[dict[str, Any]]] = {}
        by_exit_reason: dict[str, list[dict[str, Any]]] = {}
        by_strategy_and_symbol: dict[str, list[dict[str, Any]]] = {}
        for row in closed_rows:
            by_strategy.setdefault(str(row["strategy_name"]), []).append(row)
            by_symbol.setdefault(str(row["symbol"]), []).append(row)
            by_exit_reason.setdefault(str(row["exit_reason"]), []).append(row)
            by_strategy_and_symbol.setdefault(f"{row['strategy_name']}|{row['symbol']}", []).append(row)

        summary["by_strategy"] = {key: _build_group_summary(items) for key, items in sorted(by_strategy.items())}
        summary["by_symbol"] = {key: _build_group_summary(items) for key, items in sorted(by_symbol.items())}
        summary["by_exit_reason"] = {key: _build_group_summary(items) for key, items in sorted(by_exit_reason.items())}
        summary["by_strategy_and_symbol"] = {
            key: _build_group_summary(items) for key, items in sorted(by_strategy_and_symbol.items())
        }
        return summary

    def get_blocked_signal_summary(self) -> dict[str, Any]:
        if not self.conn:
            raise RuntimeError("DB not connected")

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT
                timestamp, symbol, strategy_name, decision, signal, blocked_reason,
                trade_blocked, exec_blocked
            FROM signal_audit
            WHERE signal = 'BUY'
            ORDER BY id DESC
            """
        )
        rows = cur.fetchall()

        total_buy_signals = len(rows)
        executed_buy_signals = 0
        blocked_buy_signals = 0
        blocked_by_reason: dict[str, int] = {}
        blocked_by_strategy: dict[str, int] = {}
        blocked_by_symbol: dict[str, int] = {}
        blocked_by_strategy_and_reason: dict[str, int] = {}
        buy_signals_by_strategy: dict[str, int] = {}
        executed_buy_signals_by_strategy: dict[str, int] = {}

        for row in rows:
            strategy_key = str(row["strategy_name"] or "UNKNOWN")
            buy_signals_by_strategy[strategy_key] = buy_signals_by_strategy.get(strategy_key, 0) + 1
            is_blocked = bool(row["trade_blocked"]) or bool(row["exec_blocked"]) or bool(row["blocked_reason"])
            if is_blocked:
                blocked_buy_signals += 1
                reason_key = str(row["blocked_reason"] or "UNKNOWN")
                symbol_key = str(row["symbol"] or "UNKNOWN")
                blocked_by_reason[reason_key] = blocked_by_reason.get(reason_key, 0) + 1
                blocked_by_strategy[strategy_key] = blocked_by_strategy.get(strategy_key, 0) + 1
                blocked_by_symbol[symbol_key] = blocked_by_symbol.get(symbol_key, 0) + 1
                composite_key = f"{strategy_key}|{reason_key}"
                blocked_by_strategy_and_reason[composite_key] = blocked_by_strategy_and_reason.get(composite_key, 0) + 1
            else:
                executed_buy_signals += 1
                executed_buy_signals_by_strategy[strategy_key] = executed_buy_signals_by_strategy.get(strategy_key, 0) + 1

        return {
            "total_buy_signals": total_buy_signals,
            "executed_buy_signals": executed_buy_signals,
            "blocked_buy_signals": blocked_buy_signals,
            "blocked_rate": float(blocked_buy_signals / total_buy_signals) if total_buy_signals else 0.0,
            "buy_signals_by_strategy": dict(sorted(buy_signals_by_strategy.items())),
            "executed_buy_signals_by_strategy": dict(sorted(executed_buy_signals_by_strategy.items())),
            "blocked_by_reason": dict(sorted(blocked_by_reason.items())),
            "blocked_by_strategy": dict(sorted(blocked_by_strategy.items())),
            "blocked_by_symbol": dict(sorted(blocked_by_symbol.items())),
            "blocked_by_strategy_and_reason": dict(sorted(blocked_by_strategy_and_reason.items())),
        }

    def get_session_bucket_summary(self) -> dict[str, Any]:
        if not self.conn:
            raise RuntimeError("DB not connected")

        bucket_labels = [self._session_bucket_label_from_hour(hour) for hour in range(0, 24, 3)]
        bucket_map: dict[str, dict[str, Any]] = {
            label: {
                "closed_trades": 0,
                "net_pnl_sum": 0.0,
                "win_rate": 0.0,
                "blocked_buy_signals": 0,
                "executed_buy_signals": 0,
            }
            for label in bucket_labels
        }
        trade_win_counts: dict[str, int] = {label: 0 for label in bucket_labels}

        cur = self.conn.cursor()
        cur.execute("SELECT timestamp, net_pnl, realized_pnl FROM trades WHERE side = 'SELL'")
        for row in cur.fetchall():
            bucket = self._session_bucket_label(row["timestamp"])
            if bucket not in bucket_map:
                continue
            pnl = float(self._effective_trade_pnl_value(row) or 0.0)
            bucket_map[bucket]["closed_trades"] += 1
            bucket_map[bucket]["net_pnl_sum"] += pnl
            if pnl > 0:
                trade_win_counts[bucket] += 1

        cur.execute(
            """
            SELECT timestamp, blocked_reason, trade_blocked, exec_blocked
            FROM signal_audit
            WHERE signal = 'BUY'
            """
        )
        for row in cur.fetchall():
            bucket = self._session_bucket_label(row["timestamp"])
            if bucket not in bucket_map:
                continue
            is_blocked = bool(row["trade_blocked"]) or bool(row["exec_blocked"]) or bool(row["blocked_reason"])
            if is_blocked:
                bucket_map[bucket]["blocked_buy_signals"] += 1
            else:
                bucket_map[bucket]["executed_buy_signals"] += 1

        for label, payload in bucket_map.items():
            closed_trades = int(payload["closed_trades"])
            payload["net_pnl_sum"] = float(payload["net_pnl_sum"])
            payload["win_rate"] = float(trade_win_counts[label] / closed_trades) if closed_trades else 0.0
        return bucket_map

    def get_open_positions_snapshot(self) -> list[dict[str, Any]]:
        positions = self.get_open_positions()
        now_dt = datetime.now(tz=UTC_TZ).replace(tzinfo=None)
        snapshot: list[dict[str, Any]] = []
        for position in positions:
            entry_time = position.get("opened_at") or position.get("updated_at")
            parsed_entry = self._parse_stored_timestamp(entry_time)
            age_minutes = None
            if parsed_entry is not None:
                try:
                    parsed_entry_naive = parsed_entry.astimezone(UTC_TZ).replace(tzinfo=None) if parsed_entry.tzinfo else parsed_entry
                    age_minutes = max(0.0, (now_dt - parsed_entry_naive).total_seconds() / 60.0)
                except Exception:
                    age_minutes = None
            snapshot.append(
                {
                    "symbol": position.get("symbol"),
                    "strategy_name": position.get("strategy_name"),
                    "entry_time": entry_time,
                    "age_minutes": None if age_minutes is None else round(float(age_minutes), 2),
                    "entry_price": float(position.get("entry_price") or 0.0),
                    "regime": position.get("regime"),
                    "dir_1h": position.get("dir_1h"),
                }
            )
        snapshot.sort(key=lambda item: float(item.get("age_minutes") or 0.0), reverse=True)
        return snapshot

    def get_report_health(self, recent_hours: int = 24) -> dict[str, Any]:
        if not self.conn:
            raise RuntimeError("DB not connected")

        recent_hours_i = max(1, int(recent_hours))
        cutoff_key = (datetime.now(tz=UTC_TZ) - timedelta(hours=recent_hours_i)).replace(tzinfo=None).isoformat(timespec="seconds")
        cur = self.conn.cursor()

        cur.execute("SELECT COUNT(*) AS cnt FROM signal_audit")
        signal_audit_row_count = int(cur.fetchone()["cnt"])

        cur.execute("SELECT COUNT(*) AS cnt FROM trades WHERE side = 'SELL'")
        closed_trade_row_count = int(cur.fetchone()["cnt"])

        cur.execute("SELECT COUNT(*) AS cnt FROM signal_audit WHERE signal = 'BUY' AND timestamp >= ?", (cutoff_key,))
        recent_signal_audit_buy_count = int(cur.fetchone()["cnt"])

        cur.execute("SELECT COUNT(*) AS cnt FROM trades WHERE side = 'SELL' AND timestamp >= ?", (cutoff_key,))
        recent_closed_sell_count = int(cur.fetchone()["cnt"])

        validation = self.get_trade_audit_validation(recent_hours=recent_hours_i, top_blocked_limit=5)
        legacy_null_count = int(validation["legacy_closed_rows_with_null_strategy_count"])

        return {
            "signal_audit_row_count": signal_audit_row_count,
            "closed_trade_row_count": closed_trade_row_count,
            "recent_window_hours": recent_hours_i,
            "recent_signal_audit_buy_count": recent_signal_audit_buy_count,
            "recent_closed_sell_count": recent_closed_sell_count,
            "has_legacy_closed_rows_with_null_strategy": bool(legacy_null_count > 0),
            "legacy_closed_rows_with_null_strategy_count": legacy_null_count,
        }

    def get_signal_audit_validation(self, preview_limit: int = 5, include_smoke: bool = False) -> dict[str, Any]:
        if not self.conn:
            raise RuntimeError("DB not connected")

        preview_limit_i = max(1, int(preview_limit))
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'signal_audit'")
        table_exists = cur.fetchone() is not None

        payload: dict[str, Any] = {
            "table_exists": bool(table_exists),
            "total_rows": 0,
            "smoke_row_count": 0,
            "real_row_count_excluding_smoke": 0,
            "preview_includes_smoke": bool(include_smoke),
            "latest_rows_preview": [],
            "signal_eq_buy_count": 0,
            "decision_like_buy_count": 0,
            "exec_blocked_count": 0,
            "trade_blocked_count": 0,
        }
        if not table_exists:
            return payload

        cur.execute("SELECT COUNT(*) AS cnt FROM signal_audit")
        payload["total_rows"] = int(cur.fetchone()["cnt"])

        cur.execute("SELECT COUNT(*) AS cnt FROM signal_audit WHERE strategy_name = '__codex_smoke__'")
        payload["smoke_row_count"] = int(cur.fetchone()["cnt"])
        payload["real_row_count_excluding_smoke"] = max(0, int(payload["total_rows"]) - int(payload["smoke_row_count"]))

        cur.execute("SELECT COUNT(*) AS cnt FROM signal_audit WHERE signal = 'BUY'")
        payload["signal_eq_buy_count"] = int(cur.fetchone()["cnt"])

        cur.execute("SELECT COUNT(*) AS cnt FROM signal_audit WHERE decision LIKE 'BUY%'")
        payload["decision_like_buy_count"] = int(cur.fetchone()["cnt"])

        cur.execute("SELECT COUNT(*) AS cnt FROM signal_audit WHERE COALESCE(exec_blocked, 0) = 1")
        payload["exec_blocked_count"] = int(cur.fetchone()["cnt"])

        cur.execute("SELECT COUNT(*) AS cnt FROM signal_audit WHERE COALESCE(trade_blocked, 0) = 1")
        payload["trade_blocked_count"] = int(cur.fetchone()["cnt"])

        preview_sql = """
            SELECT id, timestamp, symbol, decision, signal, strategy_name, blocked_reason, trade_blocked, exec_blocked
            FROM signal_audit
        """
        preview_params: tuple[Any, ...] = ()
        if not include_smoke:
            preview_sql += " WHERE COALESCE(strategy_name, '') != '__codex_smoke__'"
        preview_sql += " ORDER BY id DESC LIMIT ?"
        preview_params = (preview_limit_i,)
        cur.execute(preview_sql, preview_params)
        payload["latest_rows_preview"] = [
            {
                "id": int(row["id"]),
                "timestamp": row["timestamp"],
                "symbol": row["symbol"],
                "decision": row["decision"],
                "signal": row["signal"],
                "strategy_name": row["strategy_name"],
                "blocked_reason": row["blocked_reason"],
                "trade_blocked": int(row["trade_blocked"] or 0),
                "exec_blocked": int(row["exec_blocked"] or 0),
            }
            for row in cur.fetchall()
        ]
        return payload

    def get_trade_audit_validation(self, recent_hours: int = 24, top_blocked_limit: int = 5) -> dict[str, Any]:
        if not self.conn:
            raise RuntimeError("DB not connected")

        recent_hours_i = max(1, int(recent_hours))
        top_blocked_limit_i = max(1, int(top_blocked_limit))
        cutoff_key = (datetime.now(tz=UTC_TZ) - timedelta(hours=recent_hours_i)).replace(tzinfo=None).isoformat(timespec="seconds")

        any_metadata_expr = """
            strategy_name IS NOT NULL
            OR entry_reason IS NOT NULL
            OR exit_reason IS NOT NULL
            OR regime IS NOT NULL
            OR atr_pct IS NOT NULL
            OR dir_1h IS NOT NULL
            OR entry_price IS NOT NULL
            OR exit_price IS NOT NULL
            OR entry_cost IS NOT NULL
            OR exit_cost IS NOT NULL
            OR entry_fee IS NOT NULL
            OR exit_fee IS NOT NULL
            OR total_fees IS NOT NULL
            OR gross_pnl IS NOT NULL
            OR net_pnl IS NOT NULL
        """
        patch_era_missing_expr = """
            strategy_name IS NULL
            OR entry_reason IS NULL
            OR exit_reason IS NULL
            OR entry_price IS NULL
            OR exit_price IS NULL
            OR entry_cost IS NULL
            OR exit_cost IS NULL
            OR entry_fee IS NULL
            OR exit_fee IS NULL
            OR total_fees IS NULL
            OR gross_pnl IS NULL
            OR net_pnl IS NULL
        """

        cur = self.conn.cursor()

        cur.execute("SELECT COUNT(*) AS cnt FROM signal_audit WHERE signal = 'BUY'")
        signal_audit_buy_count = int(cur.fetchone()["cnt"])

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM signal_audit
            WHERE signal = 'BUY'
              AND (
                COALESCE(trade_blocked, 0) = 1
                OR COALESCE(exec_blocked, 0) = 1
                OR blocked_reason IS NOT NULL
              )
            """
        )
        blocked_buy_count = int(cur.fetchone()["cnt"])
        executed_buy_count = max(0, signal_audit_buy_count - blocked_buy_count)

        cur.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(blocked_reason), ''), 'UNKNOWN') AS reason_key,
                COUNT(*) AS cnt
            FROM signal_audit
            WHERE signal = 'BUY'
              AND (
                COALESCE(trade_blocked, 0) = 1
                OR COALESCE(exec_blocked, 0) = 1
                OR blocked_reason IS NOT NULL
              )
            GROUP BY reason_key
            ORDER BY cnt DESC, reason_key ASC
            LIMIT ?
            """,
            (top_blocked_limit_i,),
        )
        top_blocked_reasons = {str(row["reason_key"]): int(row["cnt"]) for row in cur.fetchall()}

        cur.execute("SELECT COUNT(*) AS cnt FROM trades WHERE side = 'SELL' AND strategy_name IS NULL")
        closed_trades_missing_strategy_name = int(cur.fetchone()["cnt"])

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM trades
            WHERE side = 'SELL'
              AND (
                entry_price IS NULL
                OR exit_price IS NULL
                OR net_pnl IS NULL
              )
            """
        )
        closed_trades_missing_entry_exit_or_net_pnl = int(cur.fetchone()["cnt"])

        cur.execute(
            f"""
            SELECT MIN(id) AS first_id
            FROM trades
            WHERE side = 'SELL' AND ({any_metadata_expr})
            """
        )
        first_patch_era_closed_trade_id_row = cur.fetchone()
        first_patch_era_closed_trade_id = (
            int(first_patch_era_closed_trade_id_row["first_id"])
            if first_patch_era_closed_trade_id_row and first_patch_era_closed_trade_id_row["first_id"] is not None
            else None
        )

        patch_era_closed_trade_count = 0
        patch_era_closed_trades_with_missing_attribution_or_accounting = 0
        legacy_closed_rows_with_null_strategy_count = closed_trades_missing_strategy_name
        patch_era_status = "not_observable_yet"
        newly_closed_trades_preserve_strategy_name_and_accounting: bool | None = None

        if first_patch_era_closed_trade_id is not None:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM trades WHERE side = 'SELL' AND id >= ?",
                (first_patch_era_closed_trade_id,),
            )
            patch_era_closed_trade_count = int(cur.fetchone()["cnt"])

            cur.execute(
                f"""
                SELECT COUNT(*) AS cnt
                FROM trades
                WHERE side = 'SELL'
                  AND id >= ?
                  AND ({patch_era_missing_expr})
                """,
                (first_patch_era_closed_trade_id,),
            )
            patch_era_closed_trades_with_missing_attribution_or_accounting = int(cur.fetchone()["cnt"])

            cur.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM trades
                WHERE side = 'SELL'
                  AND id < ?
                  AND strategy_name IS NULL
                """,
                (first_patch_era_closed_trade_id,),
            )
            legacy_closed_rows_with_null_strategy_count = int(cur.fetchone()["cnt"])

            patch_era_status = (
                "healthy"
                if patch_era_closed_trades_with_missing_attribution_or_accounting == 0
                else "needs_attention"
            )
            newly_closed_trades_preserve_strategy_name_and_accounting = (
                patch_era_closed_trades_with_missing_attribution_or_accounting == 0
            )

        cur.execute("SELECT COUNT(*) AS cnt FROM signal_audit WHERE signal = 'BUY' AND timestamp >= ?", (cutoff_key,))
        recent_signal_audit_buy_count = int(cur.fetchone()["cnt"])

        cur.execute("SELECT COUNT(*) AS cnt FROM trades WHERE side = 'SELL' AND timestamp >= ?", (cutoff_key,))
        recent_closed_sell_count = int(cur.fetchone()["cnt"])

        blocked_signal_summary = self.get_blocked_signal_summary()
        report_total_buy_signal_count = int(blocked_signal_summary.get("total_buy_signals", 0) or 0)
        report_blocked_buy_count = int(blocked_signal_summary.get("blocked_buy_signals", 0) or 0)

        return {
            "recent_window_hours": recent_hours_i,
            "signal_audit_buy_count": signal_audit_buy_count,
            "blocked_buy_count": blocked_buy_count,
            "executed_buy_count": executed_buy_count,
            "top_blocked_reasons": top_blocked_reasons,
            "closed_trades_missing_strategy_name": closed_trades_missing_strategy_name,
            "closed_trades_missing_entry_price_exit_price_or_net_pnl": closed_trades_missing_entry_exit_or_net_pnl,
            "first_patch_era_closed_trade_id_with_any_metadata": first_patch_era_closed_trade_id,
            "patch_era_closed_trade_count": patch_era_closed_trade_count,
            "patch_era_closed_trades_with_missing_attribution_or_accounting": (
                patch_era_closed_trades_with_missing_attribution_or_accounting
            ),
            "patch_era_status": patch_era_status,
            "newly_closed_trades_preserve_strategy_name_and_accounting": (
                newly_closed_trades_preserve_strategy_name_and_accounting
            ),
            "legacy_closed_rows_with_null_strategy_count": legacy_closed_rows_with_null_strategy_count,
            "legacy_closed_rows_still_exist": bool(legacy_closed_rows_with_null_strategy_count > 0),
            "recent_signal_audit_buy_count": recent_signal_audit_buy_count,
            "recent_closed_sell_count": recent_closed_sell_count,
            "signal_audit_receiving_rows": bool(recent_signal_audit_buy_count > 0),
            "report_total_buy_signal_count": report_total_buy_signal_count,
            "report_blocked_buy_count": report_blocked_buy_count,
            "blocked_buy_counts_match_report": bool(
                report_total_buy_signal_count == signal_audit_buy_count and report_blocked_buy_count == blocked_buy_count
            ),
            "blocked_buy_signals_visible_in_report": bool(report_blocked_buy_count > 0),
        }

    def get_trade_measurement_report(
        self,
        recent_closed_limit: int = 20,
        report_health: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        closed_trade_summary = self.get_closed_trade_analytics()
        effective_report_health = report_health or self.get_report_health()
        return {
            "summary_overall": {
                key: value
                for key, value in closed_trade_summary.items()
                if key not in {"by_strategy", "by_symbol", "by_exit_reason", "by_strategy_and_symbol"}
            },
            "by_strategy": closed_trade_summary.get("by_strategy", {}),
            "by_symbol": closed_trade_summary.get("by_symbol", {}),
            "by_exit_reason": closed_trade_summary.get("by_exit_reason", {}),
            "by_strategy_and_symbol": closed_trade_summary.get("by_strategy_and_symbol", {}),
            "blocked_signal_summary": self.get_blocked_signal_summary(),
            "session_buckets": self.get_session_bucket_summary(),
            "recent_closed_trades": self.get_closed_trade_exports(limit=recent_closed_limit),
            "open_positions_snapshot": self.get_open_positions_snapshot(),
            "report_health": effective_report_health,
            "report_meta": {
                "timestamp_assumption": "stored naive timestamps are bucketed as persisted without timezone conversion",
            },
        }

    def get_dashboard_summary(self) -> dict[str, Any]:
        """Thread-safe dashboard summary — kendi bağlantısını açar."""
        import sqlite3 as _sqlite3
        try:
            conn = _sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.row_factory = _sqlite3.Row
            cur = conn.cursor()

            # trades tablosu kolon listesi
            cur.execute("PRAGMA table_info(trades)")
            pragma_rows = cur.fetchall()
            try:
                trade_columns = {str(r["name"]) for r in pragma_rows}
            except Exception:
                trade_columns = {str(r[1]) for r in pragma_rows if len(r) > 1}

            pnl_expr = self._effective_trade_pnl_expr(trade_columns) or "0.0"

            cur.execute(f"SELECT COALESCE(SUM({pnl_expr}), 0) AS realized_pnl FROM trades")
            row = cur.fetchone()
            realized = float(row[0] if row else 0)

            cur.execute("SELECT COUNT(*) AS cnt FROM trades")
            row = cur.fetchone()
            trade_count = int(row[0] if row else 0)

            cur.execute("SELECT COUNT(*) AS cnt FROM positions")
            row = cur.fetchone()
            open_positions = int(row[0] if row else 0)

            conn.close()
            return {
                "realized_pnl": realized,
                "total_pnl": realized,
                "trade_count": trade_count,
                "open_positions": open_positions,
            }
        except Exception as e:
            logger.error("get_dashboard_summary error: %s", e)
            return {"realized_pnl": 0.0, "total_pnl": 0.0, "trade_count": 0, "open_positions": 0}