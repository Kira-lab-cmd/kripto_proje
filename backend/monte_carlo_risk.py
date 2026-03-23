from __future__ import annotations

import random
import statistics
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MCRiskConfig:
    n_sims: int = 5000
    horizon_trades: int | None = None
    block_size: int = 1
    ruin_floor_pct: float = 0.5
    cost_mult: float = 1.0
    pnl_shrink: float = 1.0
    loss_tail_mult: float = 1.0
    seed: int = 7


@dataclass(frozen=True)
class MCRiskResult:
    n_sims: int
    horizon_trades: int
    initial_equity: float
    final_equity_stats: dict[str, float]
    max_dd_stats: dict[str, float]
    prob_ruin: float
    prob_profit: float
    worst_10_avg_final_equity: float
    dd_series_summary: list[tuple[int, float]]
    notes: list[str]


def _trade_get(trade: Any, key: str, default: Any = None) -> Any:
    if isinstance(trade, dict):
        return trade.get(key, default)
    return getattr(trade, key, default)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    xs = sorted(float(v) for v in values)
    if len(xs) == 1:
        return float(xs[0])
    p = max(0.0, min(1.0, float(q)))
    pos = p * (len(xs) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    w = pos - lo
    return float(xs[lo] * (1.0 - w) + xs[hi] * w)


def extract_trade_returns(
    trades: list[Any],
    mode: str = "pnl",
    *,
    cost_mult: float = 1.0,
) -> list[float]:
    out: list[float] = []
    m = str(mode or "pnl").strip().lower()
    cm = float(cost_mult)
    for t in trades or []:
        if m == "r":
            out.append(_as_float(_trade_get(t, "r_multiple"), 0.0))
            continue
        pnl = _as_float(_trade_get(t, "realized_pnl"), 0.0)
        fee = _as_float(_trade_get(t, "fee_paid"), 0.0)
        extra_cost = fee * (cm - 1.0)
        out.append(float(pnl - extra_cost))
    return out


def bootstrap_path(
    returns: list[float],
    config: MCRiskConfig,
    *,
    rng: random.Random,
    horizon_trades: int | None = None,
) -> list[float]:
    if not returns:
        raise ValueError("no_trades")
    horizon = int(horizon_trades if horizon_trades is not None else (config.horizon_trades or len(returns)))
    horizon = max(1, horizon)
    n = len(returns)
    block = max(1, int(config.block_size))

    if block == 1:
        return [float(returns[rng.randrange(n)]) for _ in range(horizon)]

    path: list[float] = []
    while len(path) < horizon:
        start = rng.randrange(n)
        for i in range(block):
            idx = (start + i) % n
            path.append(float(returns[idx]))
            if len(path) >= horizon:
                break
    return path


def simulate_equity_curve(
    returns: list[float],
    initial_equity: float,
    config: MCRiskConfig,
) -> tuple[float, float, float]:
    eq = float(initial_equity)
    peak = float(initial_equity)
    min_eq = float(initial_equity)
    max_dd = 0.0
    shrink = float(config.pnl_shrink)
    loss_tail_mult = float(config.loss_tail_mult)
    for r in returns:
        pnl = float(r) * shrink
        if pnl < 0:
            pnl *= loss_tail_mult
        eq += pnl
        if eq > peak:
            peak = eq
        if eq < min_eq:
            min_eq = eq
        if peak > 0:
            dd = (peak - eq) / peak
            if dd > max_dd:
                max_dd = dd
    return float(eq), float(max_dd), float(min_eq)


def run_monte_carlo(
    trades: list[Any],
    initial_equity: float,
    config: MCRiskConfig,
    mode: str = "pnl",
) -> MCRiskResult:
    returns = extract_trade_returns(trades, mode=mode, cost_mult=float(config.cost_mult))
    if not returns:
        raise ValueError("no_trades")

    horizon = int(config.horizon_trades or len(returns))
    horizon = max(1, horizon)
    n_sims = max(1, int(config.n_sims))
    rng = random.Random(int(config.seed))
    ruin_floor = float(initial_equity) * float(config.ruin_floor_pct)

    finals: list[float] = []
    max_dds: list[float] = []
    ruins = 0
    profits = 0

    for _ in range(n_sims):
        path = bootstrap_path(returns, config, rng=rng, horizon_trades=horizon)
        final_eq, max_dd, min_eq = simulate_equity_curve(path, float(initial_equity), config)
        finals.append(float(final_eq))
        max_dds.append(float(max_dd))
        if min_eq < ruin_floor:
            ruins += 1
        if final_eq > float(initial_equity):
            profits += 1

    finals_sorted = sorted(finals)
    worst_k = finals_sorted[: min(10, len(finals_sorted))]
    notes = [
        f"mode={str(mode).lower()}",
        f"bootstrap_block_size={int(config.block_size)}",
        "pnl_mode_assumes_realized_pnl_is_fee_inclusive_and_applies_cost_mult_as_extra_fee_stress",
    ]
    if str(mode).lower() == "r":
        notes.append("r_mode_treats_r_multiple_as_additive_path_units")

    return MCRiskResult(
        n_sims=n_sims,
        horizon_trades=horizon,
        initial_equity=float(initial_equity),
        final_equity_stats={
            "mean": float(statistics.fmean(finals)),
            "median": float(statistics.median(finals)),
            "p05": float(_percentile(finals, 0.05)),
            "p95": float(_percentile(finals, 0.95)),
        },
        max_dd_stats={
            "mean": float(statistics.fmean(max_dds)),
            "median": float(statistics.median(max_dds)),
            "p95": float(_percentile(max_dds, 0.95)),
            "p99": float(_percentile(max_dds, 0.99)),
        },
        prob_ruin=float(ruins / n_sims),
        prob_profit=float(profits / n_sims),
        worst_10_avg_final_equity=float(statistics.fmean(worst_k)),
        dd_series_summary=[
            (50, float(_percentile(max_dds, 0.50))),
            (90, float(_percentile(max_dds, 0.90))),
            (95, float(_percentile(max_dds, 0.95))),
            (99, float(_percentile(max_dds, 0.99))),
        ],
        notes=notes,
    )
