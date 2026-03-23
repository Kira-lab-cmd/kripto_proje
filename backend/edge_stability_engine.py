from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EdgeStabilityConfig:
    objective_lambda: float = 100.0


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _extract_trade_rows(data: Any) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    if isinstance(data, list):
        for t in data:
            if isinstance(t, dict):
                rows.append(
                    {
                        "ts_ms": _f(t.get("entry_ts_ms"), 0.0),
                        "pnl": _f(t.get("realized_pnl"), 0.0),
                        "r": _f(t.get("r_multiple"), 0.0),
                    }
                )
        return rows

    if not isinstance(data, dict):
        return rows

    trades = data.get("trades")
    if isinstance(trades, list):
        return _extract_trade_rows(trades)

    for key in ("overlay", "base"):
        node = data.get(key)
        if isinstance(node, dict) and isinstance(node.get("trades"), list):
            return _extract_trade_rows(node.get("trades") or [])

    folds = data.get("folds")
    if isinstance(folds, list):
        for i, fold in enumerate(folds):
            if not isinstance(fold, dict):
                continue
            tm = fold.get("test_metrics") or {}
            rows.append(
                {
                    "ts_ms": _f(fold.get("test_end_ms"), float(i)),
                    "pnl": _f(tm.get("net_pnl"), 0.0),
                    "r": _f(tm.get("avg_r_multiple"), 0.0),
                }
            )
    return rows


def _max_drawdown_proxy(pnls: list[float]) -> float:
    eq = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnls:
        eq += float(p)
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
    return float(max_dd)


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    xs = sorted(float(v) for v in values)
    n = len(xs)
    mid = n // 2
    if n % 2:
        return float(xs[mid])
    return float((xs[mid - 1] + xs[mid]) / 2.0)


def _p(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    xs = sorted(float(v) for v in values)
    if len(xs) == 1:
        return float(xs[0])
    pos = max(0.0, min(1.0, float(q))) * (len(xs) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    w = pos - lo
    return float(xs[lo] * (1.0 - w) + xs[hi] * w)


def evaluate_edge_stability(data: Any, config: EdgeStabilityConfig | None = None) -> dict[str, Any]:
    cfg = config or EdgeStabilityConfig()
    rows = _extract_trade_rows(data)
    if not rows:
        return {
            "trade_count": 0,
            "net_pnl": 0.0,
            "win_rate": 0.0,
            "avg_r": 0.0,
            "pnl_std": 0.0,
            "max_dd_proxy": 0.0,
            "objective": 0.0,
            "stability_score": 0.0,
            "tail_pnl": {"p10": 0.0, "p50": 0.0, "p90": 0.0},
            "notes": ["no_rows"],
        }

    rows = sorted(rows, key=lambda r: int(r["ts_ms"]))
    pnls = [float(r["pnl"]) for r in rows]
    rs = [float(r["r"]) for r in rows]
    n = len(rows)
    wins = len([x for x in pnls if x > 0])
    net = float(sum(pnls))
    avg_r = float(sum(rs) / n) if n else 0.0
    mean_pnl = float(net / n) if n else 0.0
    var = float(sum((x - mean_pnl) ** 2 for x in pnls) / n) if n else 0.0
    pnl_std = var ** 0.5
    dd = _max_drawdown_proxy(pnls)
    objective = float(net - float(cfg.objective_lambda) * dd)
    scale = max(1.0, abs(net) + dd)
    stability_score = float(objective / scale)

    return {
        "trade_count": int(n),
        "net_pnl": float(net),
        "win_rate": float(wins / n) if n else 0.0,
        "avg_r": float(avg_r),
        "pnl_std": float(pnl_std),
        "max_dd_proxy": float(dd),
        "objective": float(objective),
        "stability_score": float(stability_score),
        "tail_pnl": {
            "p10": float(_p(pnls, 0.10)),
            "p50": float(_median(pnls)),
            "p90": float(_p(pnls, 0.90)),
        },
        "notes": [
            "deterministic_summary",
            "uses_trades_if_available_else_walkforward_fold_net_pnl",
        ],
    }
