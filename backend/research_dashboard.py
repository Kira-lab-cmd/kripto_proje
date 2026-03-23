from __future__ import annotations

from typing import Any

from .edge_stability_engine import evaluate_edge_stability


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _replay_summary(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"available": False}
    metrics = payload.get("metrics") or {}
    trades = payload.get("trades") or []
    if not isinstance(trades, list):
        trades = []
    return {
        "available": True,
        "initial_equity": _f(payload.get("initial_equity"), 0.0),
        "final_equity": _f(payload.get("final_equity"), 0.0),
        "trade_count": int(_f(metrics.get("trade_count"), len(trades))),
        "net_pnl": _f(metrics.get("net_pnl"), 0.0),
        "win_rate": _f(metrics.get("win_rate"), 0.0),
        "avg_r_multiple": _f(metrics.get("avg_r_multiple"), 0.0),
    }


def _walkforward_summary(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"available": False}
    agg = payload.get("aggregated") or {}
    eq = payload.get("equity_curve_summary") or {}
    return {
        "available": True,
        "fold_count": int(_f(agg.get("fold_count"), 0.0)),
        "total_test_net_pnl": _f(agg.get("total_test_net_pnl"), 0.0),
        "avg_test_win_rate": _f(agg.get("avg_test_win_rate"), 0.0),
        "avg_test_trade_count": _f(agg.get("avg_test_trade_count"), 0.0),
        "avg_test_max_dd_pct": _f(agg.get("avg_test_max_dd_pct"), 0.0),
        "initial_equity": _f(eq.get("initial_equity"), 0.0),
        "final_equity": _f(eq.get("final_equity"), 0.0),
    }


def build_research_dashboard(
    *,
    replay_payload: dict[str, Any] | None = None,
    walkforward_payload: dict[str, Any] | None = None,
    include_edge_stability: bool = True,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "replay": _replay_summary(replay_payload),
        "walkforward": _walkforward_summary(walkforward_payload),
    }
    if include_edge_stability:
        source = replay_payload if isinstance(replay_payload, dict) else walkforward_payload
        out["edge_stability"] = evaluate_edge_stability(source)

    replay_net = _f((out["replay"] or {}).get("net_pnl"), 0.0)
    wf_net = _f((out["walkforward"] or {}).get("total_test_net_pnl"), 0.0)
    highlights: list[str] = []
    if out["replay"].get("available"):
        highlights.append(f"replay_net_pnl={replay_net:.6f}")
    if out["walkforward"].get("available"):
        highlights.append(f"walkforward_total_test_net_pnl={wf_net:.6f}")
    if "edge_stability" in out:
        highlights.append(f"edge_stability_score={_f(out['edge_stability'].get('stability_score')):.6f}")
    out["highlights"] = highlights
    return out
