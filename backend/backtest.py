# backend/backtest.py
import logging
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import ccxt
import pandas as pd

from .core.indicators_light import ema, slope_normalized
from .research_store import ResearchStore
from .strategy import GATE_STATUS_KEYS, TradingStrategy

logger = logging.getLogger(__name__)


def _get_opportunity_horizon_bars() -> int:
    try:
        return max(1, int(os.getenv("OPPORTUNITY_HORIZON_BARS", "12") or "12"))
    except Exception:
        return 12


OPPORTUNITY_HORIZON_BARS = _get_opportunity_horizon_bars()
HOLD_REJECTION_REASON_CODES = (
    "regime_no_data",
    "reason_insufficient_data",
    "current_price_missing",
    "atr_missing",
    "ohlcv_too_short",
    "gate_status_missing",
    "hold_fail_reasons_missing",
    "hold_fail_reasons_mismatch",
)
NO_DATA_HOLD_REJECTION_CODES = {
    "regime_no_data",
    "reason_insufficient_data",
    "current_price_missing",
    "atr_missing",
    "ohlcv_too_short",
}
BACKEND_DIR = Path(__file__).resolve().parent


def _empty_gate_fail_stats() -> dict[str, int]:
    return {name: 0 for name in GATE_STATUS_KEYS}


def _compute_trend_bias_dir_from_1h_closes(
    closes: list[float],
    *,
    ema_fast: int = 50,
    ema_slow: int = 200,
    slope_lookback: int = 20,
    slope_min: float = 0.001,
) -> str:
    if len(closes) < max(ema_fast, ema_slow) + slope_lookback + 5:
        return "UNKNOWN"
    ema50 = ema(closes, ema_fast)
    ema200 = ema(closes, ema_slow)
    if not ema50 or not ema200:
        return "UNKNOWN"
    close_last = float(closes[-1])
    ema50_last = float(ema50[-1])
    ema200_last = float(ema200[-1])
    slope = float(slope_normalized(ema50, slope_lookback))
    if close_last > ema200_last and ema50_last > ema200_last and slope > slope_min:
        return "UP"
    if close_last < ema200_last and ema50_last < ema200_last and slope < -slope_min:
        return "DOWN"
    return "NEUTRAL"


def _resolve_research_db_path() -> Path | None:
    candidates: list[Path] = []
    raw_env_path = str(os.getenv("RESEARCH_DB_PATH", "") or "").strip()
    if raw_env_path:
        env_path = Path(raw_env_path)
        if env_path.is_absolute():
            candidates.append(env_path)
        else:
            candidates.extend(
                [
                    BACKEND_DIR / env_path,
                    BACKEND_DIR.parent / env_path,
                    Path.cwd() / env_path,
                ]
            )

    candidates.extend(
        [
            BACKEND_DIR / "research.db",
            BACKEND_DIR.parent / "research.db",
        ]
    )

    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.exists():
            return resolved
    return None


def _load_backtest_1h_context(symbol: str, df_15m: pd.DataFrame) -> pd.DataFrame:
    db_path = _resolve_research_db_path()
    if db_path is not None:
        try:
            rows = ResearchStore(str(db_path)).load(symbol, "1h")
        except Exception as exc:
            logger.warning("Failed to load 1H context from %s: %s", db_path, exc)
        else:
            if rows:
                return pd.DataFrame(
                    {
                        "ts_ms": [int(r.ts_ms) for r in rows],
                        "close": [float(r.close) for r in rows],
                    }
                ).sort_values("ts_ms", kind="stable").reset_index(drop=True)

    ts_utc = pd.to_datetime(df_15m["timestamp"], unit="ms", utc=True, errors="coerce")
    fallback_df = pd.DataFrame(
        {
            "datetime_utc": ts_utc,
            "close": pd.to_numeric(df_15m["close"], errors="coerce"),
        }
    ).dropna(subset=["datetime_utc", "close"])
    if fallback_df.empty:
        return pd.DataFrame(columns=["ts_ms", "close"])

    resampled = (
        fallback_df.set_index("datetime_utc")
        .resample("1h", label="right", closed="right")
        .last()
        .dropna(subset=["close"])
        .reset_index()
    )
    return pd.DataFrame(
        {
            "ts_ms": (resampled["datetime_utc"].astype("int64") // 10**6).astype("int64"),
            "close": resampled["close"].astype("float64"),
        }
    )


def _accumulate_gate_fail_stats(stats: dict[str, int], gate_status: dict[str, Any] | None) -> None:
    data = gate_status or {}
    for name in GATE_STATUS_KEYS:
        if not bool(data.get(name, False)):
            stats[name] = int(stats.get(name, 0)) + 1


def _build_hold_opportunity(
    *,
    symbol: str,
    timestamp,
    ref_price: float | None,
    atr: float | None,
    hold_fail_reasons: list[str] | None,
    gate_status: dict[str, Any] | None,
    reason: str | None,
    future_bars,
    atr_sl_mult: float,
    atr_tp_mult: float,
    horizon_bars: int,
) -> dict[str, Any]:
    gate_status_data = {name: bool((gate_status or {}).get(name, False)) for name in GATE_STATUS_KEYS}
    fail_reasons = list(hold_fail_reasons or [name for name, ok in gate_status_data.items() if not ok])

    ref_price_value = float(ref_price) if ref_price is not None else None
    atr_value = float(atr) if atr is not None else None
    hypothetical_sl = None
    hypothetical_tp = None
    outcome = "unresolved"

    if ref_price_value is not None and ref_price_value > 0 and atr_value is not None and atr_value > 0:
        hypothetical_sl = ref_price_value - (float(atr_sl_mult) * atr_value)
        hypothetical_tp = ref_price_value + (float(atr_tp_mult) * atr_value)
        for _, row in future_bars.iterrows():
            low = float(row["low"])
            high = float(row["high"])
            if low <= hypothetical_sl:
                outcome = "would_stop_out"
                break
            if high >= hypothetical_tp:
                outcome = "missed_tp"
                break

    return {
        "symbol": str(symbol),
        "timestamp": timestamp,
        "ref_price": ref_price_value,
        "atr": atr_value,
        "hold_fail_reasons": fail_reasons,
        "gate_status": gate_status_data,
        "reason": str(reason or ""),
        "hypothetical_entry": ref_price_value,
        "hypothetical_sl": hypothetical_sl,
        "hypothetical_tp": hypothetical_tp,
        "horizon_bars": int(horizon_bars),
        "outcome": outcome,
    }


def _empty_opportunity_outcome_counts() -> dict[str, int]:
    return {
        "total_hold_opportunities": 0,
        "missed_tp_count": 0,
        "would_stop_out_count": 0,
        "unresolved_count": 0,
    }


def _accumulate_opportunity_outcome(
    counts: dict[str, int],
    *,
    opportunity: dict[str, Any],
    missed_tp_by_fail_gate: dict[str, int],
    stopout_by_fail_gate: dict[str, int],
) -> None:
    counts["total_hold_opportunities"] = int(counts.get("total_hold_opportunities", 0)) + 1
    outcome = str(opportunity.get("outcome") or "unresolved")
    fail_reasons = list(opportunity.get("hold_fail_reasons") or [])

    if outcome == "missed_tp":
        counts["missed_tp_count"] = int(counts.get("missed_tp_count", 0)) + 1
        for gate_name in fail_reasons:
            if gate_name in missed_tp_by_fail_gate:
                missed_tp_by_fail_gate[gate_name] = int(missed_tp_by_fail_gate.get(gate_name, 0)) + 1
        return

    if outcome == "would_stop_out":
        counts["would_stop_out_count"] = int(counts.get("would_stop_out_count", 0)) + 1
        for gate_name in fail_reasons:
            if gate_name in stopout_by_fail_gate:
                stopout_by_fail_gate[gate_name] = int(stopout_by_fail_gate.get(gate_name, 0)) + 1
        return

    counts["unresolved_count"] = int(counts.get("unresolved_count", 0)) + 1


def register_hold_opportunity(
    *,
    symbol: str,
    timestamp,
    current_price: float | None,
    atr: float | None,
    gate_status: dict[str, Any] | None,
    hold_fail_reasons: list[str] | None,
    reason: str | None,
    future_bars,
    atr_sl_mult: float,
    atr_tp_mult: float,
    horizon_bars: int,
    gate_fail_stats: dict[str, int],
    hold_opportunities: list[dict[str, Any]],
    opportunity_counts: dict[str, int],
    missed_tp_by_fail_gate: dict[str, int],
    stopout_by_fail_gate: dict[str, int],
) -> None:
    _accumulate_gate_fail_stats(gate_fail_stats, gate_status)
    opportunity = _build_hold_opportunity(
        symbol=symbol,
        timestamp=timestamp,
        ref_price=current_price,
        atr=atr,
        hold_fail_reasons=hold_fail_reasons,
        gate_status=gate_status,
        reason=reason,
        future_bars=future_bars,
        atr_sl_mult=atr_sl_mult,
        atr_tp_mult=atr_tp_mult,
        horizon_bars=horizon_bars,
    )
    hold_opportunities.append(opportunity)
    _accumulate_opportunity_outcome(
        opportunity_counts,
        opportunity=opportunity,
        missed_tp_by_fail_gate=missed_tp_by_fail_gate,
        stopout_by_fail_gate=stopout_by_fail_gate,
    )


def _empty_hold_rejection_stats() -> dict[str, int]:
    return {code: 0 for code in HOLD_REJECTION_REASON_CODES}


def get_real_hold_rejection_reason(signal_data: dict[str, Any], ohlcv_len: int) -> str | None:
    if int(ohlcv_len) < 200:
        return "ohlcv_too_short"
    if str(signal_data.get("regime") or "") == "NO_DATA":
        return "regime_no_data"
    if str(signal_data.get("reason") or "") == "Yetersiz veri":
        return "reason_insufficient_data"
    if signal_data.get("current_price") is None:
        return "current_price_missing"
    if signal_data.get("atr") is None:
        return "atr_missing"

    gate_status = signal_data.get("gate_status")
    if not isinstance(gate_status, dict) or not gate_status:
        return "gate_status_missing"
    if any(name not in gate_status for name in GATE_STATUS_KEYS):
        return "gate_status_missing"

    real_fail_reasons = [name for name in GATE_STATUS_KEYS if not bool(gate_status.get(name, False))]
    hold_fail_reasons_raw = signal_data.get("hold_fail_reasons")
    hold_fail_reasons = list(hold_fail_reasons_raw or [])
    if not real_fail_reasons:
        return "hold_fail_reasons_missing"
    if not hold_fail_reasons:
        return "hold_fail_reasons_missing"
    if hold_fail_reasons != real_fail_reasons:
        return "hold_fail_reasons_mismatch"
    return None


def _is_real_hold_opportunity(signal_data: dict[str, Any], *, ohlcv_len: int) -> bool:
    return get_real_hold_rejection_reason(signal_data, ohlcv_len) is None


def _derive_setup_quality(*, total_trades: int, win_rate: float, expectancy_pct: float, gate_fail_stats: dict[str, int]) -> str:
    strict_fail_pressure = int(gate_fail_stats.get("breakout_ok", 0)) + int(gate_fail_stats.get("volume_ok", 0))
    if total_trades == 0 and strict_fail_pressure >= 10:
        return "TOO_STRICT"
    if total_trades > 0 and win_rate < 0.5 and expectancy_pct < 0:
        return "LOW_QUALITY_SETUPS"
    if expectancy_pct > 0:
        return "ACCEPTABLE"
    return "NEEDS_REVIEW"


def _build_summary_metrics(initial_balance, final_balance, closed_trades, max_drawdown_pct):
    total_trades = len(closed_trades)
    realized_pnls = [float(t.get("pnl_value", 0.0) or 0.0) for t in closed_trades]
    realized_pnl_pcts = [float(t.get("pnl_pct", 0.0) or 0.0) for t in closed_trades]

    wins = [p for p in realized_pnl_pcts if p > 0]
    losses = [p for p in realized_pnl_pcts if p < 0]

    win_rate = (len(wins) / total_trades) if total_trades else 0.0
    loss_rate = (len(losses) / total_trades) if total_trades else 0.0
    avg_win_pct = (sum(wins) / len(wins)) if wins else 0.0
    avg_loss_pct = (sum(losses) / len(losses)) if losses else 0.0
    expectancy_pct = ((win_rate * avg_win_pct) - (loss_rate * abs(avg_loss_pct))) if total_trades else 0.0

    gross_profit = sum(p for p in realized_pnls if p > 0)
    gross_loss = abs(sum(p for p in realized_pnls if p < 0))
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = "inf"
    else:
        profit_factor = 0.0

    net_pnl = float(final_balance - initial_balance)
    if total_trades == 0:
        verdict = "NO_TRADES"
    elif expectancy_pct > 0 and net_pnl > 0:
        verdict = "POSITIVE_EXPECTANCY"
    else:
        verdict = "NEGATIVE_EXPECTANCY"

    return {
        "net_pnl": round(net_pnl, 2),
        "total_trades": int(total_trades),
        "win_rate": round(win_rate, 4),
        "avg_win_pct": round(avg_win_pct, 4),
        "avg_loss_pct": round(avg_loss_pct, 4),
        "expectancy_pct": round(expectancy_pct, 4),
        "profit_factor": round(profit_factor, 4) if isinstance(profit_factor, float) and math.isfinite(profit_factor) else profit_factor,
        "max_drawdown_pct": round(max_drawdown_pct, 2),
        "verdict": verdict,
    }


def _build_entry_event(*, entry_time, entry_price, qty, balance_after) -> dict[str, Any]:
    return {
        "time": entry_time,
        "type": "BUY",
        "side": "BUY",
        "price": float(entry_price),
        "amount": float(qty),
        "balance": float(balance_after),
    }


def _close_position(open_position: dict[str, Any], *, exit_price: float, exit_time, exit_reason: str, commission_rate: float, balance_before: float):
    gross_revenue = float(open_position["qty"]) * float(exit_price)
    net_revenue = gross_revenue * (1 - commission_rate)
    balance_after = float(balance_before) + float(net_revenue)

    cash_spent = float(open_position["cash_spent"])
    pnl_value = float(net_revenue) - cash_spent
    pnl_pct = ((pnl_value / cash_spent) * 100) if cash_spent > 0 else 0.0

    closed_trade = {
        "time": exit_time,
        "type": "EXIT",
        "side": "BUY",
        "entry_time": open_position["entry_time"],
        "entry_price": float(open_position["entry_price"]),
        "exit_price": float(exit_price),
        "price": float(exit_price),
        "amount": float(open_position["qty"]),
        "qty": float(open_position["qty"]),
        "balance": float(balance_after),
        "pnl": float(pnl_value),
        "pnl_value": float(pnl_value),
        "pnl_pct": float(pnl_pct),
        "exit_reason": str(exit_reason),
        "stop_loss": open_position.get("stop_loss"),
        "take_profit": open_position.get("take_profit"),
    }

    return balance_after, closed_trade


def _resolve_exit_for_bar(open_position: dict[str, Any], candle_row) -> tuple[str | None, float | None]:
    stop_loss = open_position.get("stop_loss")
    take_profit = open_position.get("take_profit")
    low = float(candle_row["low"])
    high = float(candle_row["high"])

    if stop_loss is not None and low <= float(stop_loss):
        return "stop_loss_hit", float(stop_loss)
    if take_profit is not None and high >= float(take_profit):
        return "take_profit_hit", float(take_profit)
    return None, None


class Backtester:
    def __init__(
        self,
        symbol="BTC/USDT",
        timeframe="15m",
        start_date="2024-01-01",
        end_date=None,
        initial_balance=1000,
    ):
        self.symbol = symbol
        self.timeframe = timeframe
        self.start_date = start_date
        self.end_date = end_date
        self.initial_balance = initial_balance
        self.commission_rate = 0.001  # Binance Spot 0.1%
        self.strategy = TradingStrategy()
        self.exchange = ccxt.binance()

        safe_symbol = symbol.replace("/", "_")
        self.data_file = f"data/{safe_symbol}_{timeframe}.csv"

        os.makedirs("data", exist_ok=True)

    def fetch_data(self):
        """Fetch OHLCV from CSV cache or the exchange."""
        if os.path.exists(self.data_file):
            logger.info("Data loaded from CSV: %s", self.data_file)
            df = pd.read_csv(self.data_file)
            df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
            return df

        logger.info("Downloading OHLCV from exchange: %s", self.symbol)
        since = int(datetime.fromisoformat(self.start_date).timestamp() * 1000)
        end_ts = (
            int(datetime.fromisoformat(self.end_date).timestamp() * 1000)
            if self.end_date
            else int(datetime.now().timestamp() * 1000)
        )

        all_candles = []
        while since < end_ts:
            try:
                candles = self.exchange.fetch_ohlcv(self.symbol, self.timeframe, since=since, limit=1000)
                if not candles:
                    break
                all_candles.extend(candles)
                since = candles[-1][0] + 1
            except Exception as e:
                logger.error("Data fetch failed: %s", e)
                break

        if not all_candles:
            raise ValueError("No OHLCV data fetched")

        df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df.to_csv(self.data_file, index=False)
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        logger.info("Data cached: %s candles", len(df))
        return df

    def run(self):
        df = self.fetch_data()

        if len(df) < 200:
            return {"error": "Yetersiz veri (min 200 mum)"}

        balance = float(self.initial_balance)
        open_position: dict[str, Any] | None = None
        event_history: list[dict[str, Any]] = []
        closed_trades: list[dict[str, Any]] = []
        gate_fail_stats = _empty_gate_fail_stats()
        hold_opportunities: list[dict[str, Any]] = []
        opportunity_counts = _empty_opportunity_outcome_counts()
        missed_tp_by_fail_gate = _empty_gate_fail_stats()
        stopout_by_fail_gate = _empty_gate_fail_stats()
        skipped_no_data_holds = 0
        counted_real_holds = 0
        hold_rejection_stats = _empty_hold_rejection_stats()
        rejected_hold_samples: list[dict[str, Any]] = []

        peak_balance = balance
        max_drawdown = 0.0

        logger.info("Backtest simulation started")
        print("opportunity tracker active")

        min_strategy_bars = 200
        max_signal_window = 300
        bias_debug_prints_remaining = 10
        min_ohlcv_len_seen: int | None = None
        max_ohlcv_len_seen = 0
        trend_dir_1h_stats = {"UP": 0, "NEUTRAL": 0, "DOWN": 0, "UNKNOWN": 0}
        context_1h_df = _load_backtest_1h_context(self.symbol, df)
        context_1h_ts = [int(v) for v in context_1h_df.get("ts_ms", pd.Series(dtype="int64")).tolist()]
        context_1h_closes = [float(v) for v in context_1h_df.get("close", pd.Series(dtype="float64")).tolist()]
        bias_cursor = -1
        bias_cursor_dir = "UNKNOWN"
        bias_cursor_computed_at = -2

        for i in range(min_strategy_bars - 1, len(df) - 1):
            window_size = min(i + 1, max_signal_window)
            window_start = max(0, i - window_size + 1)
            current_window_df = df.iloc[window_start : i + 1]
            next_candle = df.iloc[i + 1]

            if open_position is not None:
                exit_reason, exit_price = _resolve_exit_for_bar(open_position, next_candle)
                if exit_reason is not None and exit_price is not None:
                    balance, closed_trade = _close_position(
                        open_position,
                        exit_price=exit_price,
                        exit_time=next_candle["datetime"],
                        exit_reason=exit_reason,
                        commission_rate=self.commission_rate,
                        balance_before=balance,
                    )
                    closed_trades.append(closed_trade)
                    event_history.append(closed_trade)
                    open_position = None

            if open_position is None:
                current_window_list = current_window_df[
                    ["timestamp", "open", "high", "low", "close", "volume"]
                ].values.tolist()
                current_ohlcv_len = int(len(current_window_list))
                current_ts_ms = int(current_window_list[-1][0]) if current_window_list else 0
                if min_ohlcv_len_seen is None or current_ohlcv_len < min_ohlcv_len_seen:
                    min_ohlcv_len_seen = current_ohlcv_len
                if current_ohlcv_len > max_ohlcv_len_seen:
                    max_ohlcv_len_seen = current_ohlcv_len
                while bias_cursor + 1 < len(context_1h_ts) and int(context_1h_ts[bias_cursor + 1]) <= current_ts_ms:
                    bias_cursor += 1
                if bias_cursor >= 0 and bias_cursor != bias_cursor_computed_at:
                    bias_cursor_dir = _compute_trend_bias_dir_from_1h_closes(context_1h_closes[: bias_cursor + 1])
                    bias_cursor_computed_at = bias_cursor
                elif bias_cursor < 0:
                    bias_cursor_dir = "UNKNOWN"
                    bias_cursor_computed_at = bias_cursor
                trend_dir_1h = str(bias_cursor_dir or "UNKNOWN").upper()
                if trend_dir_1h not in trend_dir_1h_stats:
                    trend_dir_1h = "UNKNOWN"
                trend_dir_1h_stats[trend_dir_1h] = int(trend_dir_1h_stats.get(trend_dir_1h, 0)) + 1
                signal_data = self.strategy.get_signal(current_window_list, 0, trend_dir_1h=trend_dir_1h)
                if bias_debug_prints_remaining > 0:
                    print(
                        "bias_debug",
                        f"timestamp={current_window_df.iloc[-1]['datetime']}",
                        f"symbol={self.symbol}",
                        f"trend_dir_1h={trend_dir_1h}",
                        f"gate_status={signal_data.get('gate_status')}",
                        f"reason={signal_data.get('reason')}",
                    )
                    bias_debug_prints_remaining -= 1
                decision = signal_data.get("signal", "HOLD")
                if decision == "BUY":
                    stop_loss = signal_data.get("stop_loss")
                    take_profit = signal_data.get("take_profit")
                    entry_price = float(next_candle["open"])
                    if stop_loss is not None and take_profit is not None:
                        cash_spent = balance * 0.95
                        qty = (cash_spent / entry_price) * (1 - self.commission_rate) if entry_price > 0 else 0.0
                        if qty > 0 and cash_spent > 0:
                            balance -= cash_spent
                            open_position = {
                                "side": "BUY",
                                "entry_time": next_candle["datetime"],
                                "entry_price": entry_price,
                                "qty": qty,
                                "cash_spent": cash_spent,
                                "stop_loss": float(stop_loss),
                                "take_profit": float(take_profit),
                            }
                            event_history.append(
                                _build_entry_event(
                                    entry_time=next_candle["datetime"],
                                    entry_price=entry_price,
                                    qty=qty,
                                    balance_after=balance,
                                )
                            )
                            exit_reason, exit_price = _resolve_exit_for_bar(open_position, next_candle)
                            if exit_reason is not None and exit_price is not None:
                                balance, closed_trade = _close_position(
                                    open_position,
                                    exit_price=exit_price,
                                    exit_time=next_candle["datetime"],
                                    exit_reason=exit_reason,
                                    commission_rate=self.commission_rate,
                                    balance_before=balance,
                                )
                                closed_trades.append(closed_trade)
                                event_history.append(closed_trade)
                                open_position = None
                        else:
                            open_position = None
                else:
                    rejection_reason = get_real_hold_rejection_reason(signal_data, len(current_window_list))
                    if rejection_reason is None:
                        register_hold_opportunity(
                            symbol=self.symbol,
                            timestamp=current_window_df.iloc[-1]["datetime"],
                            current_price=signal_data.get("current_price"),
                            atr=signal_data.get("atr"),
                            gate_status=signal_data.get("gate_status"),
                            hold_fail_reasons=signal_data.get("hold_fail_reasons"),
                            reason=signal_data.get("reason"),
                            future_bars=df.iloc[i + 1 : i + 1 + OPPORTUNITY_HORIZON_BARS],
                            atr_sl_mult=float(self.strategy.atr_sl_mult),
                            atr_tp_mult=float(self.strategy.atr_tp_mult),
                            horizon_bars=OPPORTUNITY_HORIZON_BARS,
                            gate_fail_stats=gate_fail_stats,
                            hold_opportunities=hold_opportunities,
                            opportunity_counts=opportunity_counts,
                            missed_tp_by_fail_gate=missed_tp_by_fail_gate,
                            stopout_by_fail_gate=stopout_by_fail_gate,
                        )
                        counted_real_holds += 1
                    else:
                        hold_rejection_stats[rejection_reason] = int(hold_rejection_stats.get(rejection_reason, 0)) + 1
                        if rejection_reason in NO_DATA_HOLD_REJECTION_CODES:
                            skipped_no_data_holds += 1
                        if len(rejected_hold_samples) < 5:
                            rejected_hold_samples.append(
                                {
                                    "timestamp": current_window_df.iloc[-1]["datetime"],
                                    "symbol": self.symbol,
                                    "regime": signal_data.get("regime"),
                                    "reason": signal_data.get("reason"),
                                    "current_price": signal_data.get("current_price"),
                                    "atr": signal_data.get("atr"),
                                    "ohlcv_len": int(len(current_window_list)),
                                    "gate_status": signal_data.get("gate_status"),
                                    "hold_fail_reasons": signal_data.get("hold_fail_reasons"),
                                    "rejection_reason": rejection_reason,
                                }
                            )

            marked_price = float(next_candle["close"])
            current_equity = balance + (float(open_position["qty"]) * marked_price if open_position is not None else 0.0)

            if current_equity > peak_balance:
                peak_balance = current_equity

            dd = (peak_balance - current_equity) / peak_balance * 100 if peak_balance > 0 else 0.0
            if dd > max_drawdown:
                max_drawdown = dd

        if open_position is not None:
            final_row = df.iloc[-1]
            balance, closed_trade = _close_position(
                open_position,
                exit_price=float(final_row["close"]),
                exit_time=final_row["datetime"],
                exit_reason="final_close",
                commission_rate=self.commission_rate,
                balance_before=balance,
            )
            closed_trades.append(closed_trade)
            event_history.append(closed_trade)
            open_position = None

        total_return = ((balance - self.initial_balance) / self.initial_balance) * 100
        summary_metrics = _build_summary_metrics(
            initial_balance=self.initial_balance,
            final_balance=balance,
            closed_trades=closed_trades,
            max_drawdown_pct=max_drawdown,
        )
        setup_quality = _derive_setup_quality(
            total_trades=int(summary_metrics["total_trades"]),
            win_rate=float(summary_metrics["win_rate"]),
            expectancy_pct=float(summary_metrics["expectancy_pct"]),
            gate_fail_stats=gate_fail_stats,
        )
        total_hold_opportunities = int(opportunity_counts["total_hold_opportunities"])
        missed_tp_rate = (
            float(opportunity_counts["missed_tp_count"]) / float(total_hold_opportunities)
            if total_hold_opportunities
            else 0.0
        )
        print("total_hold_opportunities=", total_hold_opportunities)
        print("skipped_no_data_holds=", skipped_no_data_holds)
        print("counted_real_holds=", counted_real_holds)
        print("hold_rejection_stats=", hold_rejection_stats)
        print("trend_dir_1h_stats=", trend_dir_1h_stats)
        print("min_ohlcv_len_seen=", min_ohlcv_len_seen)
        print("max_ohlcv_len_seen=", max_ohlcv_len_seen)

        return {
            "symbol": self.symbol,
            "initial_balance": self.initial_balance,
            "final_balance": round(balance, 2),
            "net_pnl": summary_metrics["net_pnl"],
            "total_return_percent": round(total_return, 2),
            "max_drawdown_percent": round(max_drawdown, 2),
            "max_drawdown_pct": summary_metrics["max_drawdown_pct"],
            "total_trades": summary_metrics["total_trades"],
            "win_rate": summary_metrics["win_rate"],
            "avg_win_pct": summary_metrics["avg_win_pct"],
            "avg_loss_pct": summary_metrics["avg_loss_pct"],
            "expectancy_pct": summary_metrics["expectancy_pct"],
            "profit_factor": summary_metrics["profit_factor"],
            "verdict": summary_metrics["verdict"],
            "gate_fail_stats": gate_fail_stats,
            "total_hold_opportunities": total_hold_opportunities,
            "skipped_no_data_holds": int(skipped_no_data_holds),
            "counted_real_holds": int(counted_real_holds),
            "hold_rejection_stats": hold_rejection_stats,
            "trend_dir_1h_stats": trend_dir_1h_stats,
            "rejected_hold_samples": rejected_hold_samples,
            "missed_tp_count": int(opportunity_counts["missed_tp_count"]),
            "would_stop_out_count": int(opportunity_counts["would_stop_out_count"]),
            "unresolved_count": int(opportunity_counts["unresolved_count"]),
            "missed_tp_rate": round(missed_tp_rate, 4),
            "missed_tp_by_fail_gate": missed_tp_by_fail_gate,
            "stopout_by_fail_gate": stopout_by_fail_gate,
            "setup_quality": setup_quality,
            "strategy_config": self.strategy.get_config_snapshot(),
            "trade_history": closed_trades[-10:],
            "event_history": event_history[-10:],
            "hold_opportunities": hold_opportunities[-10:],
        }


if __name__ == "__main__":
    bt = Backtester()
    print(bt.run())
