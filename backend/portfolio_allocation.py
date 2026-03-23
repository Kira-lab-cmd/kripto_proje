from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AllocationConfig:
    max_weight: float = 0.60
    min_weight: float = 0.0


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _extract_trades(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [t for t in payload if isinstance(t, dict)]
    if not isinstance(payload, dict):
        return []
    trades = payload.get("trades")
    if isinstance(trades, list):
        return [t for t in trades if isinstance(t, dict)]
    for key in ("overlay", "base"):
        node = payload.get(key)
        if isinstance(node, dict) and isinstance(node.get("trades"), list):
            return [t for t in node.get("trades") if isinstance(t, dict)]
    return []


def _extract_symbols(payload: Any) -> list[str]:
    if isinstance(payload, dict):
        symbols = payload.get("symbols")
        if isinstance(symbols, list):
            out = [str(s) for s in symbols if str(s)]
            if out:
                return sorted(set(out))
        cfg = payload.get("config")
        if isinstance(cfg, dict) and isinstance(cfg.get("symbols"), list):
            out = [str(s) for s in cfg.get("symbols") if str(s)]
            if out:
                return sorted(set(out))
    return []


def _normalize(weights: dict[str, float]) -> dict[str, float]:
    total = float(sum(max(0.0, w) for w in weights.values()))
    if total <= 0:
        n = len(weights)
        return {k: (1.0 / n if n else 0.0) for k in sorted(weights)}
    return {k: max(0.0, float(w)) / total for k, w in sorted(weights.items())}


def _cap_and_floor(weights: dict[str, float], *, max_weight: float, min_weight: float) -> dict[str, float]:
    out = {k: float(v) for k, v in weights.items()}
    out = {k: max(float(min_weight), v) for k, v in out.items()}
    out = _normalize(out)
    cap = max(0.0, min(1.0, float(max_weight)))

    for _ in range(max(1, len(out) * 2)):
        capped = {k: min(v, cap) for k, v in out.items()}
        excess = float(sum(out.values()) - sum(capped.values()))
        out = dict(capped)
        if excess <= 1e-12:
            break
        receivers = [k for k, v in out.items() if v < cap - 1e-12]
        if not receivers:
            break
        receiver_total = float(sum(out[k] for k in receivers))
        if receiver_total <= 0:
            share = excess / len(receivers)
            for k in receivers:
                out[k] += share
        else:
            for k in receivers:
                out[k] += excess * (out[k] / receiver_total)
        out = _normalize(out)
    return _normalize(out)


def allocate_portfolio(
    payload: Any,
    *,
    total_equity: float,
    config: AllocationConfig | None = None,
) -> dict[str, Any]:
    cfg = config or AllocationConfig()
    trades = _extract_trades(payload)
    stats: dict[str, dict[str, float]] = {}

    for t in trades:
        sym = str(t.get("symbol") or "")
        if not sym:
            continue
        st = stats.setdefault(sym, {"count": 0.0, "wins": 0.0, "net": 0.0, "r_sum": 0.0})
        pnl = _f(t.get("realized_pnl"), 0.0)
        r = _f(t.get("r_multiple"), 0.0)
        st["count"] += 1.0
        st["net"] += pnl
        st["r_sum"] += r
        if pnl > 0:
            st["wins"] += 1.0

    if not stats:
        syms = _extract_symbols(payload)
        if not syms:
            return {
                "total_equity": float(total_equity),
                "method": "equal_weight_fallback",
                "allocations": [],
                "notes": ["no_symbols_or_trades"],
            }
        eq_w = 1.0 / len(syms)
        return {
            "total_equity": float(total_equity),
            "method": "equal_weight_fallback",
            "allocations": [
                {"symbol": s, "weight": float(eq_w), "capital": float(total_equity) * float(eq_w), "score": 0.0}
                for s in syms
            ],
            "notes": ["no_trades_using_equal_weights"],
        }

    raw_scores: dict[str, float] = {}
    for sym, st in sorted(stats.items()):
        count = st["count"]
        win_rate = (st["wins"] / count) if count > 0 else 0.0
        avg_r = (st["r_sum"] / count) if count > 0 else 0.0
        net = st["net"]
        score = max(0.0, net) + max(0.0, avg_r) * count + win_rate
        raw_scores[sym] = float(score)

    if all(v <= 0 for v in raw_scores.values()):
        raw_scores = {k: 1.0 for k in raw_scores.keys()}

    weights = _normalize(raw_scores)
    weights = _cap_and_floor(weights, max_weight=float(cfg.max_weight), min_weight=float(cfg.min_weight))
    allocs: list[dict[str, Any]] = []
    for sym in sorted(weights.keys()):
        st = stats[sym]
        count = st["count"]
        win_rate = (st["wins"] / count) if count > 0 else 0.0
        avg_r = (st["r_sum"] / count) if count > 0 else 0.0
        w = float(weights[sym])
        allocs.append(
            {
                "symbol": sym,
                "weight": w,
                "capital": float(total_equity) * w,
                "score": float(raw_scores[sym]),
                "trade_count": int(count),
                "net_pnl": float(st["net"]),
                "win_rate": float(win_rate),
                "avg_r": float(avg_r),
            }
        )

    return {
        "total_equity": float(total_equity),
        "method": "score_weighted",
        "allocations": allocs,
        "notes": ["deterministic_symbol_scoring", "fallback_equal_weight_if_no_trades"],
    }
