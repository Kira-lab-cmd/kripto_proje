# File: backend/replay_engine.py
from __future__ import annotations

import argparse
import dataclasses
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .alpha_overlay import OverlayFeatureFlags, SignalTap
from .config import settings
from .execution_model import DefaultExecutionModel, ExecutionContext, ExecutionFill, ExecutionModel
from .overlay_policy import OverlayContext, OverlayPolicy
from .portfolio_risk import PortfolioRiskEngine
from .research_store import ResearchStore, OhlcvRow
from .strategy import GATE_STATUS_KEYS, TradingStrategy
from .risk_engine import compute_qty_from_stop
from .core.indicators_light import ema, slope_normalized


UTC = timezone.utc


def _get_opportunity_horizon_bars() -> int:
    try:
        return max(1, int(os.getenv("OPPORTUNITY_HORIZON_BARS", "12") or "12"))
    except Exception:
        return 12


OPPORTUNITY_HORIZON_BARS = _get_opportunity_horizon_bars()


def _dt_to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def _parse_date(s: str) -> datetime:
    # Accept YYYY-MM-DD or full ISO
    try:
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return datetime.fromisoformat(s).replace(tzinfo=UTC)
        d = datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=UTC)
    except Exception as e:
        raise ValueError(f"invalid date '{s}' (use YYYY-MM-DD or ISO8601)") from e


def _rows_to_ohlcv(rows: List[OhlcvRow]) -> List[List[float]]:
    return [[r.ts_ms, r.open, r.high, r.low, r.close, r.volume] for r in rows]


def _extract_entry_meta(res: Dict[str, Any]) -> Dict[str, Any]:
    eff = res.get("effective_thresholds") or {}
    return {
        "score": res.get("score"),
        "buy_th": eff.get("buy"),
        "sell_th": eff.get("sell"),
        "reason": res.get("reason"),
        "regime": res.get("regime"),
        "regime_conf": res.get("regime_conf"),
        "adx": res.get("adx"),
        "er": res.get("er"),
        "atr_pct": res.get("atr_pct"),
        "vol_ratio": res.get("vol_ratio"),
        "trend_dir_1h": res.get("dir_1h"),
        "is_uptrend": res.get("is_uptrend"),
        "ema200": res.get("ema200"),
        "atr": res.get("atr"),
    }


def _build_reason_preview(reason: str | None, *, max_parts: int = 4) -> str | None:
    if reason is None:
        return None
    text = str(reason).strip()
    if not text:
        return None

    # Presentation-only: keep the stored reason intact and collapse verbose diagnostics.
    base_reason = text.split(" | ", 1)[0].strip()
    parts = [part.strip() for part in base_reason.split(",") if part.strip()]
    if not parts:
        return base_reason

    preview = ", ".join(parts[:max_parts])
    if len(parts) > max_parts:
        return f"{preview}, ..."
    return preview


def _empty_gate_fail_stats() -> Dict[str, int]:
    return {name: 0 for name in GATE_STATUS_KEYS}


def _accumulate_gate_fail_stats(stats: Dict[str, int], gate_status: Dict[str, Any] | None) -> None:
    data = gate_status or {}
    for name in GATE_STATUS_KEYS:
        if not bool(data.get(name, False)):
            stats[name] = int(stats.get(name, 0)) + 1


def _build_hold_opportunity(
    *,
    symbol: str,
    timestamp: int,
    ref_price: float | None,
    atr: float | None,
    hold_fail_reasons: List[str] | None,
    gate_status: Dict[str, Any] | None,
    reason: str | None,
    future_rows: List[OhlcvRow],
    atr_sl_mult: float,
    atr_tp_mult: float,
    horizon_bars: int,
) -> Dict[str, Any]:
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
        for row in future_rows[:horizon_bars]:
            if float(row.low) <= hypothetical_sl:
                outcome = "would_stop_out"
                break
            if float(row.high) >= hypothetical_tp:
                outcome = "missed_tp"
                break

    return {
        "symbol": str(symbol),
        "timestamp": int(timestamp),
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


def _empty_opportunity_outcome_counts() -> Dict[str, int]:
    return {
        "total_hold_opportunities": 0,
        "missed_tp_count": 0,
        "would_stop_out_count": 0,
        "unresolved_count": 0,
    }


def _accumulate_opportunity_outcome(
    counts: Dict[str, int],
    *,
    opportunity: Dict[str, Any],
    missed_tp_by_fail_gate: Dict[str, int],
    stopout_by_fail_gate: Dict[str, int],
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


def _compute_trend_bias_dir_from_1h_closes(closes: List[float], *, ema_fast: int = 50, ema_slow: int = 200, slope_lookback: int = 20, slope_min: float = 0.001) -> str:
    # Mirrors backend/core/trend_bias.py decision rule (simplified)
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


def _bucket_float(value: float | None, edges: List[float], labels: List[str]) -> str:
    if value is None:
        return "unknown"
    for limit, label in zip(edges, labels):
        if value < limit:
            return label
    return labels[-1]


def _score_distance_bucket(trade: TradeEvent) -> str:
    if trade.score is None:
        return "unknown"
    if trade.side == "BUY" and trade.buy_th is not None:
        dist = float(trade.score) - float(trade.buy_th)
    elif trade.side == "SELL" and trade.sell_th is not None:
        dist = float(trade.sell_th) - float(trade.score)
    else:
        return "unknown"
    return _bucket_float(
        dist,
        [0.5, 1.0, 2.0, float("inf")],
        ["lt_0.5", "0.5_to_1.0", "1.0_to_2.0", "gte_2.0"],
    )


def _summarize_trade_group(group: List[TradeEvent]) -> Dict[str, Any]:
    n = len(group)
    wins = [t for t in group if t.realized_pnl > 0]
    losses = [t for t in group if t.realized_pnl < 0]
    net_pnl = float(sum(t.realized_pnl for t in group))
    avg_r = float(sum(float(t.r_multiple or 0.0) for t in group) / n) if n else 0.0
    gross_profit = float(sum(t.realized_pnl for t in wins))
    gross_loss = float(abs(sum(t.realized_pnl for t in losses)))
    profit_factor: float | str
    if gross_loss > 0:
        profit_factor = float(gross_profit / gross_loss)
    elif gross_profit > 0:
        profit_factor = "inf"
    else:
        profit_factor = 0.0
    return {
        "trade_count": n,
        "win_rate": float(len(wins) / n) if n else 0.0,
        "avg_R": avg_r,
        "net_pnl": net_pnl,
        "profit_factor": profit_factor,
    }


def compute_buckets(trades: List[TradeEvent]) -> Dict[str, Any]:
    bucket_specs = {
        "regime": lambda t: t.regime or "unknown",
        "trend_dir_1h": lambda t: t.trend_dir_1h or "unknown",
        "atr_pct": lambda t: _bucket_float(
            t.atr_pct,
            [0.003, 0.006, 0.01, 0.02, float("inf")],
            ["lt_0.3pct", "0.3_to_0.6pct", "0.6_to_1.0pct", "1.0_to_2.0pct", "gte_2.0pct"],
        ),
        "vol_ratio": lambda t: _bucket_float(
            t.vol_ratio,
            [0.6, 0.9, 1.2, 1.5, float("inf")],
            ["lt_0.60", "0.60_to_0.90", "0.90_to_1.20", "1.20_to_1.50", "gte_1.50"],
        ),
        "score_distance": _score_distance_bucket,
    }
    out: Dict[str, Any] = {}
    for name, key_fn in bucket_specs.items():
        grouped: Dict[str, List[TradeEvent]] = {}
        for trade in trades:
            key = str(key_fn(trade))
            grouped.setdefault(key, []).append(trade)
        out[name] = {key: _summarize_trade_group(group) for key, group in grouped.items()}
    return out


def _apply_execution_cost(price: float, side: str, atr_pct: float | None) -> float:
    base_slip = 0.0001
    vol_slip = max(0.0, float(atr_pct or 0.0)) * 0.5
    spread = 0.0002
    if side == "BUY":
        mult = 1.0 + (spread / 2.0) + base_slip + vol_slip
    else:
        mult = 1.0 - (spread / 2.0) - base_slip - vol_slip
    return float(price) * mult


def _append_exec_audit(reason: str | None, *, entry_fill: ExecutionFill | None, exit_fill: ExecutionFill | None) -> str | None:
    parts: list[str] = []
    if entry_fill is not None:
        parts.append(
            f"entry_exec(slip_bps={entry_fill.slippage_bps:.3f}, fee_bps={entry_fill.fee_bps:.3f}, fee={entry_fill.fee_paid:.6f})"
        )
    if exit_fill is not None:
        parts.append(
            f"exit_exec(slip_bps={exit_fill.slippage_bps:.3f}, fee_bps={exit_fill.fee_bps:.3f}, fee={exit_fill.fee_paid:.6f})"
        )
    if not parts:
        return reason
    audit = " | ".join(parts)
    return f"{reason} | {audit}" if reason else audit


def _merge_execution_notes(entry_fill: ExecutionFill | None, exit_fill: ExecutionFill | None) -> str | None:
    notes: list[str] = []
    if entry_fill is not None and entry_fill.note:
        notes.append(str(entry_fill.note))
    if exit_fill is not None and exit_fill.note:
        notes.append(str(exit_fill.note))
    return " | ".join(notes) if notes else None


class _LegacyExecutionEngineAdapter(ExecutionModel):
    def __init__(self, engine: Any):
        self._engine = engine

    def fill(self, ctx: ExecutionContext) -> ExecutionFill:
        result = self._engine.execute_market(
            side=str(ctx.action or ctx.side or "BUY"),
            ref_price=float(ctx.ref_price if ctx.ref_price is not None else (ctx.mid_price if ctx.mid_price is not None else 0.0)),
            qty=float(ctx.qty),
            note="replay_entry" if ctx.is_entry else "replay_exit",
        )
        return ExecutionFill(
            exec_price=float(result.fill_price),
            fee_paid=float(result.fee),
            slippage_bps=float(result.slippage_bps),
            fee_bps=float(result.fee_bps),
            note=(str(result.note) if getattr(result, "note", None) else None),
        )


@dataclass
class ReplayConfig:
    timeframe: str = "15m"
    bias_timeframe: str = "1h"
    warmup_bars: int = 250
    max_open_positions: int = 3
    cooldown_seconds: int = 300
    risk_per_trade: float = 0.005  # 0.5% default for replay (safer than settings default)
    commission_rate: float = 0.001
    slippage_bps: float = 2.0
    fee_rate: float = 0.001
    fee_profit_ratio: float = 2.5


@dataclass
class Position:
    symbol: str
    side: str  # "BUY" or "SELL"
    entry_ts_ms: int
    entry_price: float
    qty: float
    stop_loss: float
    take_profit: float
    risk_usdt: float  # risk budget at entry (for R-multiple)
    exit_ts_ms: int | None = None
    exit_price: float | None = None
    realized_pnl: float = 0.0
    fee_paid: float = 0.0
    r_multiple: float | None = None
    entry_score: float | None = None
    entry_buy_th: float | None = None
    entry_sell_th: float | None = None
    entry_reason: str | None = None
    entry_regime: str | None = None
    entry_regime_conf: float | None = None
    entry_adx: float | None = None
    entry_er: float | None = None
    entry_atr_pct: float | None = None
    entry_trend_dir_1h: str | None = None
    entry_vol_ratio: float | None = None
    entry_is_uptrend: bool | None = None
    entry_ema200: float | None = None
    entry_atr: float | None = None
    entry_fill_price: float | None = None
    entry_fee: float | None = None
    entry_slippage_bps: float | None = None
    entry_fee_bps: float | None = None
    entry_execution_note: str | None = None


@dataclass
class TradeEvent:
    symbol: str
    side: str
    entry_ts_ms: int
    exit_ts_ms: int
    entry_price: float
    exit_price: float
    qty: float
    realized_pnl: float
    fee_paid: float
    r_multiple: float
    regime: str
    adx: float | None
    er: float | None
    atr_pct: float | None
    trend_dir_1h: str | None
    score: float | None = None
    buy_th: float | None = None
    sell_th: float | None = None
    reason: str | None = None
    reason_preview: str | None = None
    exit_reason: str | None = None
    regime_conf: float | None = None
    vol_ratio: float | None = None
    is_uptrend: bool | None = None
    ema200: float | None = None
    atr: float | None = None
    entry_slippage_bps: float | None = None
    exit_slippage_bps: float | None = None
    entry_fee_bps: float | None = None
    exit_fee_bps: float | None = None
    execution_note: str | None = None


@dataclass
class ReplayResult:
    start_ms: int
    end_ms: int
    symbols: List[str]
    initial_equity: float
    final_equity: float
    trades: List[TradeEvent]
    metrics: Dict[str, Any]
    strategy_config: Dict[str, Any] | None = None
    buckets: Dict[str, Any] | None = None
    leak_stats: Dict[str, Any] | None = None
    walkforward: Dict[str, Any] | None = None


class ReplayEngine:
    def __init__(
        self,
        store: ResearchStore,
        strategy: TradingStrategy,
        *,
        cfg: ReplayConfig | None = None,
        overlay_policy: OverlayPolicy | None = None,
        enable_overlay: bool | None = None,
        execution_model: ExecutionModel | None = None,
        enable_execution_model: bool = True,
        execution_engine: ExecutionModel | None = None,  # backward-compat alias
        portfolio_risk_engine: PortfolioRiskEngine | None = None,
        enable_portfolio_risk: bool = False,
    ):
        self.store = store
        self.strategy = strategy
        self.overlay = overlay_policy
        self.enable_overlay = bool(enable_overlay) if enable_overlay is not None else (overlay_policy is not None)
        if execution_model is None and execution_engine is not None:
            execution_model = execution_engine
        if execution_model is not None and not hasattr(execution_model, "fill") and hasattr(execution_model, "execute_market"):
            execution_model = _LegacyExecutionEngineAdapter(execution_model)
        self.execution_model = execution_model or DefaultExecutionModel()
        self.enable_execution_model = bool(enable_execution_model)
        self.portfolio_risk_engine = portfolio_risk_engine
        self.enable_portfolio_risk = bool(enable_portfolio_risk) and (portfolio_risk_engine is not None)
        self._overlay_flags = OverlayFeatureFlags.from_env()
        self.cfg = cfg or ReplayConfig(
            max_open_positions=int(getattr(settings, "MAX_OPEN_POSITIONS", 3) or 3),
            cooldown_seconds=int(getattr(settings, "COOLDOWN_SECONDS", 300) or 300),
            commission_rate=float(getattr(settings, "COMMISSION_RATE", 0.001) or 0.001),
            slippage_bps=float(getattr(settings, "PAPER_SLIPPAGE_BPS", 2.0) or 2.0),
            fee_rate=float(getattr(settings, "COMMISSION_RATE", 0.001) or 0.001),
        )
        # override risk: settings default is 5% which is too aggressive for evaluation
        self.cfg.risk_per_trade = float(getattr(settings, "RISK_PER_TRADE", 0.005) or 0.005)
        # clamp risk for replay sanity
        self.cfg.risk_per_trade = max(0.0005, min(0.01, self.cfg.risk_per_trade))

    def run(
        self,
        symbols: List[str],
        *,
        start: datetime,
        end: datetime,
        initial_equity: float = 200.0,
        sentiment_score: float = 0.0,
        fail_on_leak: bool = False,
    ) -> ReplayResult:
        start_ms = _dt_to_ms(start)
        end_ms = _dt_to_ms(end)

        sym_bars_15: Dict[str, List[OhlcvRow]] = {}
        sym_bars_1h: Dict[str, List[OhlcvRow]] = {}

        # Load data from research store
        for s in symbols:
            rows15 = self.store.load(s, self.cfg.timeframe, since_ms=start_ms)
            rows15 = [r for r in rows15 if r.ts_ms <= end_ms]
            if len(rows15) < self.cfg.warmup_bars:
                continue
            rows1h = self.store.load(s, self.cfg.bias_timeframe, since_ms=start_ms)
            rows1h = [r for r in rows1h if r.ts_ms <= end_ms]
            sym_bars_15[s] = rows15
            sym_bars_1h[s] = rows1h

        symbols = [s for s in symbols if s in sym_bars_15]
        if not symbols:
            raise RuntimeError("no_symbols_with_sufficient_data")

        # Build union timeline (15m timestamps)
        timeline = sorted({r.ts_ms for s in symbols for r in sym_bars_15[s]})
        equity = float(initial_equity)
        positions: Dict[str, Position] = {}
        trades: List[TradeEvent] = []
        rejected_orders = 0
        portfolio_risk_blocks = 0
        portfolio_risk_scaled = 0
        gate_fail_stats = _empty_gate_fail_stats()
        hold_opportunities: List[Dict[str, Any]] = []
        opportunity_counts = _empty_opportunity_outcome_counts()
        missed_tp_by_fail_gate = _empty_gate_fail_stats()
        stopout_by_fail_gate = _empty_gate_fail_stats()
        leak_count = 0
        leak_examples: list[dict[str, int | str]] = []

        last_trade_ts_ms: int | None = None

        # Pre-build index maps for fast lookup
        bar_map_15: Dict[str, Dict[int, OhlcvRow]] = {s: {r.ts_ms: r for r in sym_bars_15[s]} for s in symbols}
        bar_index_15: Dict[str, Dict[int, int]] = {s: {r.ts_ms: idx for idx, r in enumerate(sym_bars_15[s])} for s in symbols}
        # For 1h bias, we compute on demand with cached mapping ts->dir
        bias_cache: Dict[Tuple[str, int], str] = {}

        def get_bias(symbol: str, ts_ms: int) -> str:
            key = (symbol, ts_ms)
            if key in bias_cache:
                return bias_cache[key]
            rows = sym_bars_1h.get(symbol) or []
            # take rows with ts_ms <= current
            closes: List[float] = [r.close for r in rows if r.ts_ms <= ts_ms]
            d = _compute_trend_bias_dir_from_1h_closes(closes)
            bias_cache[key] = d
            return d

        def record_leak(symbol: str, signal_ts: int, bar_ts: int) -> None:
            nonlocal leak_count
            leak_count += 1
            if len(leak_examples) < 5:
                leak_examples.append({"symbol": str(symbol), "signal_ts": int(signal_ts), "bar_ts": int(bar_ts)})

        for ts_ms in timeline:
            # 1) Update exits for open positions using current bar
            for sym in list(positions.keys()):
                bar = bar_map_15.get(sym, {}).get(ts_ms)
                if not bar:
                    continue
                pos = positions[sym]
                # Check stop/tp with intrabar extremes
                exit_reason = None
                exit_px = None

                if pos.side == "BUY":
                    if bar.low <= pos.stop_loss:
                        exit_reason = "stop_loss_hit"
                        exit_px = pos.stop_loss
                    elif bar.high >= pos.take_profit:
                        exit_reason = "take_profit_hit"
                        exit_px = pos.take_profit
                else:  # Symmetric exit handling for non-long entries.
                    if bar.high >= pos.stop_loss:
                        exit_reason = "stop_loss_hit"
                        exit_px = pos.stop_loss
                    elif bar.low <= pos.take_profit:
                        exit_reason = "take_profit_hit"
                        exit_px = pos.take_profit

                if exit_reason and exit_px is not None:
                    # Apply execution costs to the closing leg without changing exit rules.
                    close_action_side = "SELL" if pos.side == "BUY" else "BUY"
                    entry_fill: ExecutionFill | None = None
                    exit_fill: ExecutionFill | None = None
                    if not self.enable_execution_model:
                        exec_exit_px = _apply_execution_cost(exit_px, close_action_side, pos.entry_atr_pct)
                        gross = (exec_exit_px - pos.entry_price) * pos.qty if pos.side == "BUY" else (pos.entry_price - exec_exit_px) * pos.qty
                        fee = self.cfg.commission_rate * (abs(exec_exit_px * pos.qty) + abs(pos.entry_price * pos.qty))
                        pnl = gross - fee
                        equity += pnl
                        fee_paid = fee
                    else:
                        entry_fill = ExecutionFill(
                            exec_price=float(pos.entry_fill_price or pos.entry_price),
                            fee_paid=float(pos.entry_fee or 0.0),
                            slippage_bps=float(pos.entry_slippage_bps or 0.0),
                            fee_bps=float(pos.entry_fee_bps or 0.0),
                            note=pos.entry_execution_note,
                        )
                        exit_fill = self.execution_model.fill(
                            ExecutionContext(
                                symbol=pos.symbol,
                                ts_ms=ts_ms,
                                action=close_action_side,
                                qty=float(pos.qty),
                                ref_price=float(exit_px),
                                atr_pct=pos.entry_atr_pct,
                                is_entry=False,
                                side_position=pos.side,
                                regime=pos.entry_regime,
                                vol_ratio=pos.entry_vol_ratio,
                                trend_dir_1h=pos.entry_trend_dir_1h,
                            )
                        )
                        if bool(exit_fill.rejected):
                            rejected_orders += 1
                            exec_exit_px = float(exit_px)
                            exit_fee = 0.0
                        else:
                            exec_exit_px = float(exit_fill.exec_price)
                            exit_fee = float(exit_fill.fee_paid)
                        gross = (exec_exit_px - pos.entry_price) * pos.qty if pos.side == "BUY" else (pos.entry_price - exec_exit_px) * pos.qty
                        pnl = gross - exit_fee
                        equity += pnl
                        fee_paid = float(pos.fee_paid + exit_fee)

                    r_mult = pnl / pos.risk_usdt if pos.risk_usdt > 0 else 0.0

                    pos.exit_ts_ms = ts_ms
                    pos.exit_price = exec_exit_px
                    pos.realized_pnl = pnl
                    pos.fee_paid = fee_paid
                    pos.r_multiple = r_mult

                    # Preserve signal diagnostics captured at entry for replay analytics.
                    reason_out = _append_exec_audit(pos.entry_reason, entry_fill=entry_fill, exit_fill=exit_fill)
                    trade_event = TradeEvent(
                        symbol=pos.symbol,
                        side=pos.side,
                        entry_ts_ms=pos.entry_ts_ms,
                        exit_ts_ms=ts_ms,
                        entry_price=pos.entry_price,
                        exit_price=exec_exit_px,
                        qty=pos.qty,
                        realized_pnl=float(pnl),
                        fee_paid=float(fee_paid),
                        r_multiple=float(r_mult),
                        regime=str(pos.entry_regime or "UNKNOWN"),
                        adx=pos.entry_adx,
                        er=pos.entry_er,
                        atr_pct=pos.entry_atr_pct,
                        trend_dir_1h=str(pos.entry_trend_dir_1h or "UNKNOWN"),
                        score=pos.entry_score,
                        buy_th=pos.entry_buy_th,
                        sell_th=pos.entry_sell_th,
                        reason=reason_out,
                        reason_preview=_build_reason_preview(reason_out),
                        exit_reason=exit_reason,
                        regime_conf=pos.entry_regime_conf,
                        vol_ratio=pos.entry_vol_ratio,
                        is_uptrend=pos.entry_is_uptrend,
                        ema200=pos.entry_ema200,
                        atr=pos.entry_atr,
                        entry_slippage_bps=float(entry_fill.slippage_bps) if entry_fill else None,
                        exit_slippage_bps=float(exit_fill.slippage_bps) if exit_fill else None,
                        entry_fee_bps=float(entry_fill.fee_bps) if entry_fill else None,
                        exit_fee_bps=float(exit_fill.fee_bps) if exit_fill else None,
                        execution_note=_merge_execution_notes(entry_fill, exit_fill),
                    )
                    trades.append(trade_event)
                    del positions[sym]

            # 2) If max positions reached, skip entries but keep scanning (telemetry in live; here just skip)
            if len(positions) >= self.cfg.max_open_positions:
                continue

            # 3) Enforce global cooldown between entries
            if last_trade_ts_ms is not None:
                if ts_ms - last_trade_ts_ms < self.cfg.cooldown_seconds * 1000:
                    continue

            # 4) Generate signals and pick best candidate (highest score gap)
            best: Tuple[str, Dict[str, Any], float] | None = None
            for sym in symbols:
                if sym in positions:
                    continue
                bar = bar_map_15[sym].get(ts_ms)
                if not bar:
                    continue
                # Build rolling window up to this ts
                rows15 = sym_bars_15[sym]
                # Find index by ts (linear scan is slow; but we can slice by using map and count)
                # We'll compute window by filtering <= ts_ms and taking last warmup_bars
                # For performance, this is acceptable for modest sizes. (Optimization later.)
                window_rows = [r for r in rows15 if r.ts_ms <= ts_ms]
                if len(window_rows) < self.cfg.warmup_bars:
                    continue
                window = _rows_to_ohlcv(window_rows[-self.cfg.warmup_bars:])
                if window and int(window[-1][0]) > int(ts_ms):
                    record_leak(sym, int(ts_ms), int(window[-1][0]))
                    if fail_on_leak:
                        raise RuntimeError(f"data_leak_detected symbol={sym} signal_ts={ts_ms} bar_ts={window[-1][0]}")

                dir_1h = get_bias(sym, ts_ms)
                res = self.strategy.get_signal(
                    window,
                    sentiment_score,
                    symbol=sym,
                    trend_dir_1h=dir_1h,
                )
                tap = SignalTap.from_strategy_res(res) if self._overlay_flags.signal_tap else None
                entry_meta = _extract_entry_meta(res)
                decision = str(res.get("signal") or "HOLD")
                score = float(res.get("score") or 0.0)
                ov = None

                if self.enable_overlay and self.overlay is not None:
                    eff = res.get("effective_thresholds") or {}
                    buy_th = eff.get("buy")
                    sell_th = eff.get("sell")
                    if buy_th is None:
                        buy_th = res.get("buy_th")
                    if sell_th is None:
                        sell_th = res.get("sell_th")

                    ctx = OverlayContext(
                        symbol=sym,
                        ts_ms=int(ts_ms),
                        side=decision,
                        score=res.get("score"),
                        buy_th=buy_th,
                        sell_th=sell_th,
                        atr_pct=res.get("atr_pct"),
                        adx=res.get("adx"),
                        er=res.get("er"),
                        regime=res.get("regime"),
                        trend_dir_1h=res.get("dir_1h") or res.get("trend_dir_1h"),
                        current_price=float(res.get("current_price") or bar.close),
                        stop_loss=res.get("stop_loss"),
                        take_profit=res.get("take_profit"),
                        qty_prelim=None,
                        risk_pct=float(self.cfg.risk_per_trade),
                        base_risk_pct=float(self.cfg.risk_per_trade),
                        reason=res.get("reason"),
                    )
                    ov = self.overlay.decide(ctx)
                    buy_th_eff = float(buy_th) + float(ov.buy_th_add) if buy_th is not None else None
                    sell_th_eff = float(sell_th) - float(ov.sell_th_add) if sell_th is not None else None
                    if buy_th_eff is not None:
                        entry_meta["buy_th"] = buy_th_eff
                    if sell_th_eff is not None:
                        entry_meta["sell_th"] = sell_th_eff

                    if decision == "BUY":
                        try:
                            if ov.block_buy:
                                decision = "HOLD"
                            elif buy_th_eff is not None and score < buy_th_eff:
                                decision = "HOLD"
                        except Exception:
                            pass
                    elif decision == "SELL":
                        try:
                            if ov.block_sell:
                                decision = "HOLD"
                            elif sell_th_eff is not None and score > sell_th_eff:
                                decision = "HOLD"
                        except Exception:
                            pass

                if decision not in ("BUY", "SELL"):
                    if str(res.get("regime") or "") != "NO_DATA":
                        _accumulate_gate_fail_stats(gate_fail_stats, res.get("gate_status"))
                        bar_idx = bar_index_15.get(sym, {}).get(ts_ms)
                        future_rows = sym_bars_15[sym][bar_idx + 1 : bar_idx + 1 + OPPORTUNITY_HORIZON_BARS] if bar_idx is not None else []
                        opportunity = _build_hold_opportunity(
                            symbol=sym,
                            timestamp=int(ts_ms),
                            ref_price=res.get("current_price") or bar.close,
                            atr=res.get("atr"),
                            hold_fail_reasons=res.get("hold_fail_reasons"),
                            gate_status=res.get("gate_status"),
                            reason=res.get("reason"),
                            future_rows=future_rows,
                            atr_sl_mult=float(self.strategy.atr_sl_mult),
                            atr_tp_mult=float(self.strategy.atr_tp_mult),
                            horizon_bars=OPPORTUNITY_HORIZON_BARS,
                        )
                        hold_opportunities.append(opportunity)
                        _accumulate_opportunity_outcome(
                            opportunity_counts,
                            opportunity=opportunity,
                            missed_tp_by_fail_gate=missed_tp_by_fail_gate,
                            stopout_by_fail_gate=stopout_by_fail_gate,
                        )
                    continue

                # Only consider if stop/tp exist
                sl = res.get("stop_loss")
                tp = res.get("take_profit")
                px = float(res.get("current_price") or bar.close)

                if sl is None or tp is None:
                    continue

                # Gap = how far beyond threshold (proxy). Not perfect but ok.
                gap = abs(score)
                if best is None or gap > best[2]:
                    best = (sym, {**res, "_entry_meta": entry_meta, "_signal_tap": tap, "_overlay_decision": ov}, gap)

            if not best:
                continue

            sym, res, _ = best
            decision = str(res["signal"])
            px = float(res.get("current_price") or bar_map_15[sym][ts_ms].close)
            sl = float(res["stop_loss"])
            tp = float(res["take_profit"])
            entry_meta = dict(res.get("_entry_meta") or _extract_entry_meta(res))
            tap = res.get("_signal_tap")
            ov = res.get("_overlay_decision")
            if ov is not None and getattr(ov, "note", ""):
                ov_note = f"overlay:{ov.note}"
                base_reason = entry_meta.get("reason")
                entry_meta["reason"] = f"{base_reason} | {ov_note}" if base_reason else ov_note

            # Size by risk vs stop distance using the shared risk engine contract.
            base_risk_pct = float(self.cfg.risk_per_trade)
            overlay_scalar = 1.0
            if ov is not None:
                try:
                    overlay_scalar = float(getattr(ov, "risk_scalar", 1.0))
                    overlay_scalar = max(0.0, min(2.0, overlay_scalar))
                except Exception:
                    pass
            portfolio_scalar = 1.0
            # HOOK: PORTFOLIO_RISK (after overlay, before sizing)
            if self.enable_portfolio_risk and self.portfolio_risk_engine is not None:
                stop_dist_probe = abs(float(px) - float(sl))
                qty_probe = 0.0
                if stop_dist_probe > 0 and float(px) > 0:
                    risk_pct_probe = float(base_risk_pct) * float(overlay_scalar)
                    risk_usdt_probe = float(equity) * float(risk_pct_probe)
                    qty_probe = risk_usdt_probe / stop_dist_probe
                    max_notional_pct = float(getattr(self.cfg, "max_notional_pct", 0.25))
                    if max_notional_pct > 0:
                        max_notional = float(equity) * max_notional_pct
                        qty_probe = min(qty_probe, max_notional / float(px))
                pr = self.portfolio_risk_engine.evaluate_entry(
                    symbol=str(sym),
                    side=str(decision),
                    qty_prelim=float(max(0.0, qty_probe)),
                    price=float(px),
                    positions=positions,
                    equity=float(equity),
                    ts_ms=int(ts_ms),
                )
                pr_note = f"PORTFOLIO_RISK:{pr.note}"
                if not bool(pr.allow_entry):
                    portfolio_risk_blocks += 1
                    res["blocked_reason"] = pr_note
                    base_reason = entry_meta.get("reason")
                    entry_meta["reason"] = f"{base_reason} | {pr_note}" if base_reason else pr_note
                    continue
                try:
                    portfolio_scalar = float(pr.risk_scalar)
                    portfolio_scalar = max(0.0, min(2.0, portfolio_scalar))
                except Exception:
                    portfolio_scalar = 1.0
                if abs(float(portfolio_scalar) - 1.0) > 1e-12:
                    portfolio_risk_scaled += 1
                if pr.note and str(pr.note).strip() and str(pr.note).strip().lower() != "ok":
                    base_reason = entry_meta.get("reason")
                    entry_meta["reason"] = f"{base_reason} | {pr_note}" if base_reason else pr_note
            total_risk_scalar = max(0.0, min(2.0, float(overlay_scalar) * float(portfolio_scalar)))
            risk_pct = float(base_risk_pct) * float(total_risk_scalar)
            sr = compute_qty_from_stop(
                symbol=sym,
                entry_price=float(px),
                stop_loss=sl,
                equity_usdt=float(equity),
                risk_pct=float(risk_pct),
                exchange=None,
                max_notional_pct=float(getattr(self.cfg, "max_notional_pct", 0.25)),
            )
            risk_budget = float(sr.risk_usdt)
            qty_prelim = float(sr.qty)
            if qty_prelim <= 0:
                continue
            qty = qty_prelim

            entry_fill: ExecutionFill | None = None
            entry_fee_bps: float | None = None
            if not self.enable_execution_model:
                exec_px = _apply_execution_cost(px, decision, entry_meta.get("atr_pct"))
                notional = abs(exec_px * qty)
                fee = self.cfg.commission_rate * notional
                entry_slippage_bps = None
                entry_exec_note = None
            else:
                entry_fill = self.execution_model.fill(
                    ExecutionContext(
                        symbol=sym,
                        ts_ms=ts_ms,
                        action=decision,
                        qty=float(qty_prelim),
                        ref_price=float(px),
                        atr_pct=res.get("atr_pct"),
                        is_entry=True,
                        side_position=decision,
                        regime=res.get("regime"),
                        vol_ratio=res.get("vol_ratio"),
                        trend_dir_1h=res.get("dir_1h"),
                    )
                )
                qty = float(entry_fill.filled_qty if entry_fill.filled_qty is not None else qty_prelim)
                if bool(entry_fill.rejected) or qty <= 0.0:
                    rejected_orders += 1
                    continue
                exec_px = float(entry_fill.exec_price)
                notional = abs(exec_px * qty)
                fee = float(entry_fill.fee_paid)
                entry_slippage_bps = float(entry_fill.slippage_bps)
                entry_fee_bps = float(entry_fill.fee_bps)
                entry_exec_note = entry_fill.note
            expected_profit = abs(tp - exec_px) * qty
            if entry_fee_bps is not None:
                estimated_fee = notional * (entry_fee_bps / 10_000.0) * 2.0
            else:
                estimated_fee = notional * float(self.cfg.fee_rate) * 2.0
            if expected_profit < estimated_fee * float(self.cfg.fee_profit_ratio):
                res["blocked_reason"] = "blocked: expected_profit < fee threshold"
                continue
            equity -= fee  # fee paid on entry
            risk_usdt = abs(exec_px - sl) * qty  # approximate

            pos = Position(
                symbol=sym,
                side=decision,
                entry_ts_ms=ts_ms,
                entry_price=exec_px,
                qty=qty,
                stop_loss=sl,
                take_profit=tp,
                risk_usdt=risk_usdt if risk_usdt > 0 else risk_budget,
                fee_paid=fee,
                entry_score=entry_meta.get("score"),
                entry_buy_th=entry_meta.get("buy_th"),
                entry_sell_th=entry_meta.get("sell_th"),
                entry_reason=entry_meta.get("reason"),
                entry_regime=entry_meta.get("regime"),
                entry_regime_conf=entry_meta.get("regime_conf"),
                entry_adx=entry_meta.get("adx"),
                entry_er=entry_meta.get("er"),
                entry_atr_pct=entry_meta.get("atr_pct"),
                entry_trend_dir_1h=entry_meta.get("trend_dir_1h"),
                entry_vol_ratio=entry_meta.get("vol_ratio"),
                entry_is_uptrend=entry_meta.get("is_uptrend"),
                entry_ema200=entry_meta.get("ema200"),
                entry_atr=entry_meta.get("atr"),
                entry_fill_price=exec_px,
                entry_fee=fee,
                entry_slippage_bps=entry_slippage_bps,
                entry_fee_bps=entry_fee_bps,
                entry_execution_note=entry_exec_note,
            )
            if self._overlay_flags.signal_tap and isinstance(tap, SignalTap):
                tap.apply_to_position(pos)
            positions[sym] = pos
            last_trade_ts_ms = ts_ms

        # close remaining positions at last close (mark-to-market)
        last_ts = timeline[-1]
        for sym, pos in list(positions.items()):
            bar = bar_map_15.get(sym, {}).get(last_ts)
            if not bar:
                continue
            close_action_side = "SELL" if pos.side == "BUY" else "BUY"
            entry_fill: ExecutionFill | None = None
            exit_fill: ExecutionFill | None = None
            if not self.enable_execution_model:
                exec_exit_px = _apply_execution_cost(bar.close, close_action_side, pos.entry_atr_pct)
                gross = (exec_exit_px - pos.entry_price) * pos.qty if pos.side == "BUY" else (pos.entry_price - exec_exit_px) * pos.qty
                fee = self.cfg.commission_rate * (abs(exec_exit_px * pos.qty))
                pnl = gross - fee
                equity += pnl
                fee_paid = float(pos.fee_paid + fee)
            else:
                entry_fill = ExecutionFill(
                    exec_price=float(pos.entry_fill_price or pos.entry_price),
                    fee_paid=float(pos.entry_fee or 0.0),
                    slippage_bps=float(pos.entry_slippage_bps or 0.0),
                    fee_bps=float(pos.entry_fee_bps or 0.0),
                    note=pos.entry_execution_note,
                )
                exit_fill = self.execution_model.fill(
                    ExecutionContext(
                        symbol=pos.symbol,
                        ts_ms=last_ts,
                        action=close_action_side,
                        qty=float(pos.qty),
                        ref_price=float(bar.close),
                        atr_pct=pos.entry_atr_pct,
                        is_entry=False,
                        side_position=pos.side,
                        regime=pos.entry_regime,
                        vol_ratio=pos.entry_vol_ratio,
                        trend_dir_1h=pos.entry_trend_dir_1h,
                    )
                )
                if bool(exit_fill.rejected):
                    rejected_orders += 1
                    exec_exit_px = float(bar.close)
                    exit_fee = 0.0
                else:
                    exec_exit_px = float(exit_fill.exec_price)
                    exit_fee = float(exit_fill.fee_paid)
                gross = (exec_exit_px - pos.entry_price) * pos.qty if pos.side == "BUY" else (pos.entry_price - exec_exit_px) * pos.qty
                pnl = gross - exit_fee
                equity += pnl
                fee_paid = float(pos.fee_paid + exit_fee)
            r_mult = pnl / pos.risk_usdt if pos.risk_usdt > 0 else 0.0
            reason_out = _append_exec_audit(pos.entry_reason, entry_fill=entry_fill, exit_fill=exit_fill)
            trade_event = TradeEvent(
                symbol=pos.symbol,
                side=pos.side,
                entry_ts_ms=pos.entry_ts_ms,
                exit_ts_ms=last_ts,
                entry_price=pos.entry_price,
                exit_price=exec_exit_px,
                qty=pos.qty,
                realized_pnl=float(pnl),
                fee_paid=float(fee_paid),
                r_multiple=float(r_mult),
                regime=str(pos.entry_regime or "UNKNOWN"),
                adx=pos.entry_adx,
                er=pos.entry_er,
                atr_pct=pos.entry_atr_pct,
                trend_dir_1h=str(pos.entry_trend_dir_1h or "UNKNOWN"),
                score=pos.entry_score,
                buy_th=pos.entry_buy_th,
                sell_th=pos.entry_sell_th,
                reason=reason_out,
                reason_preview=_build_reason_preview(reason_out),
                exit_reason="final_close",
                regime_conf=pos.entry_regime_conf,
                vol_ratio=pos.entry_vol_ratio,
                is_uptrend=pos.entry_is_uptrend,
                ema200=pos.entry_ema200,
                atr=pos.entry_atr,
                entry_slippage_bps=float(entry_fill.slippage_bps) if entry_fill else None,
                exit_slippage_bps=float(exit_fill.slippage_bps) if exit_fill else None,
                entry_fee_bps=float(entry_fill.fee_bps) if entry_fill else None,
                exit_fee_bps=float(exit_fill.fee_bps) if exit_fill else None,
                execution_note=_merge_execution_notes(entry_fill, exit_fill),
            )
            trades.append(trade_event)
            del positions[sym]

        if fail_on_leak and leak_count > 0:
            raise RuntimeError(f"data_leak_detected count={leak_count} examples={leak_examples}")

        metrics = _compute_metrics(initial_equity, equity, trades, rejected_orders=rejected_orders)
        metrics["gate_fail_stats"] = gate_fail_stats
        total_hold_opportunities = int(opportunity_counts["total_hold_opportunities"])
        metrics["total_hold_opportunities"] = total_hold_opportunities
        metrics["missed_tp_count"] = int(opportunity_counts["missed_tp_count"])
        metrics["would_stop_out_count"] = int(opportunity_counts["would_stop_out_count"])
        metrics["unresolved_count"] = int(opportunity_counts["unresolved_count"])
        metrics["missed_tp_rate"] = (
            round(float(opportunity_counts["missed_tp_count"]) / float(total_hold_opportunities), 4)
            if total_hold_opportunities
            else 0.0
        )
        metrics["missed_tp_by_fail_gate"] = missed_tp_by_fail_gate
        metrics["stopout_by_fail_gate"] = stopout_by_fail_gate
        metrics["hold_opportunities_sample"] = hold_opportunities[-10:]
        if self.enable_portfolio_risk:
            metrics["portfolio_risk_blocks"] = int(portfolio_risk_blocks)
            metrics["portfolio_risk_scaled"] = int(portfolio_risk_scaled)
        buckets = compute_buckets(trades)
        return ReplayResult(
            start_ms=start_ms,
            end_ms=end_ms,
            symbols=symbols,
            initial_equity=float(initial_equity),
            final_equity=float(equity),
            trades=trades,
            metrics=metrics,
            strategy_config=self.strategy.get_config_snapshot(),
            buckets=buckets,
            leak_stats={"leak_count": int(leak_count), "examples": leak_examples},
        )


def _compute_metrics(initial_equity: float, final_equity: float, trades: List[TradeEvent], *, rejected_orders: int = 0) -> Dict[str, Any]:
    pnl = final_equity - initial_equity
    n = len(trades)
    wins = [t for t in trades if t.realized_pnl > 0]
    losses = [t for t in trades if t.realized_pnl < 0]
    win_rate = (len(wins) / n) if n else 0.0
    avg_r = (sum(t.r_multiple for t in trades) / n) if n else 0.0
    avg_win = (sum(t.realized_pnl for t in wins) / len(wins)) if wins else 0.0
    avg_loss = (sum(t.realized_pnl for t in losses) / len(losses)) if losses else 0.0
    profit_factor = (sum(t.realized_pnl for t in wins) / abs(sum(t.realized_pnl for t in losses))) if losses else float("inf") if wins else 0.0

    return {
        "initial_equity": float(initial_equity),
        "final_equity": float(final_equity),
        "net_pnl": float(pnl),
        "trade_count": int(n),
        "win_rate": float(win_rate),
        "avg_r_multiple": float(avg_r),
        "avg_win_usdt": float(avg_win),
        "avg_loss_usdt": float(avg_loss),
        "profit_factor": float(profit_factor) if math.isfinite(profit_factor) else "inf",
        "rejected_orders": int(rejected_orders),
    }
