from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


UTC = timezone.utc


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _ms_to_iso(ms: Any) -> str | None:
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=UTC).date().isoformat()
    except Exception:
        return None


def _fmt_params(params: dict[str, Any]) -> str:
    if not isinstance(params, dict):
        return str(params)
    buy = params.get("buy_th")
    sell = params.get("sell_th")
    sl = params.get("atr_sl_mult")
    tp = params.get("atr_tp_mult")
    if buy is not None and sell is not None and sl is not None and tp is not None:
        return f"(buy={buy} sell={sell} sl={sl} tp={tp})"
    return json.dumps(params, sort_keys=True)


def _collect_execution_stats(folds: list[dict[str, Any]]) -> dict[str, float | None]:
    entry_slips: list[float] = []
    exit_slips: list[float] = []
    fee_bps: list[float] = []

    for fold in folds:
        trades: list[dict[str, Any]] = []
        raw_test = fold.get("test_trades")
        raw_trades = fold.get("trades")
        if isinstance(raw_test, list):
            trades.extend([t for t in raw_test if isinstance(t, dict)])
        if isinstance(raw_trades, list):
            trades.extend([t for t in raw_trades if isinstance(t, dict)])

        for t in trades:
            if t.get("entry_slippage_bps") is not None:
                entry_slips.append(_f(t.get("entry_slippage_bps")))
            if t.get("exit_slippage_bps") is not None:
                exit_slips.append(_f(t.get("exit_slippage_bps")))
            if t.get("entry_fee_bps") is not None:
                fee_bps.append(_f(t.get("entry_fee_bps")))
            if t.get("exit_fee_bps") is not None:
                fee_bps.append(_f(t.get("exit_fee_bps")))

    return {
        "avg_entry_slippage_bps": (sum(entry_slips) / len(entry_slips)) if entry_slips else None,
        "avg_exit_slippage_bps": (sum(exit_slips) / len(exit_slips)) if exit_slips else None,
        "avg_fee_bps": (sum(fee_bps) / len(fee_bps)) if fee_bps else None,
    }


def analyze(payload: dict[str, Any]) -> dict[str, Any]:
    folds = [f for f in (payload.get("folds") or []) if isinstance(f, dict)]
    if not folds:
        raise RuntimeError("no_folds_in_input")

    nets: list[float] = []
    wins: list[float] = []
    trades: list[float] = []
    max_dds: list[float] = []
    pf = Counter()

    for fold in folds:
        tm = fold.get("test_metrics") or {}
        nets.append(_f(tm.get("net_pnl")))
        wins.append(_f(tm.get("win_rate")))
        trades.append(_f(tm.get("trade_count")))
        max_dds.append(_f(fold.get("test_max_dd_pct")))
        pf[_fmt_params(dict(fold.get("best_params") or {}))] += 1

    total_net_pnl = float(sum(nets))
    avg_win_rate = float(sum(wins) / len(wins)) if wins else 0.0
    avg_trade_count = float(sum(trades) / len(trades)) if trades else 0.0
    avg_max_dd_pct = float(sum(max_dds) / len(max_dds)) if max_dds else 0.0
    std_test_net_pnl = float(statistics.pstdev(nets)) if len(nets) > 1 else 0.0
    worst_fold_net_pnl = float(min(nets)) if nets else 0.0
    best_fold_net_pnl = float(max(nets)) if nets else 0.0
    worst_fold_max_dd = float(max(max_dds)) if max_dds else 0.0

    best_idx = max(range(len(nets)), key=lambda i: nets[i]) if nets else 0
    worst_idx = min(range(len(nets)), key=lambda i: nets[i]) if nets else 0
    best_fold = folds[best_idx]
    worst_fold = folds[worst_idx]

    eq_sum = payload.get("equity_curve_summary") or {}
    final_equity = _f(eq_sum.get("final_equity"), default=_f(eq_sum.get("initial_equity")) + total_net_pnl)

    out = {
        "summary": {
            "fold_count": len(folds),
            "total_net_pnl": total_net_pnl,
            "final_equity": final_equity,
            "avg_win_rate": avg_win_rate,
            "avg_trade_count": avg_trade_count,
            "avg_max_dd_pct": avg_max_dd_pct,
        },
        "stability": {
            "std_test_net_pnl": std_test_net_pnl,
            "worst_fold_net_pnl": worst_fold_net_pnl,
            "best_fold_net_pnl": best_fold_net_pnl,
            "worst_fold_max_dd": worst_fold_max_dd,
        },
        "best_fold": {
            "fold": int(best_fold.get("fold", best_idx)),
            "test_start": _ms_to_iso(best_fold.get("test_start_ms")),
            "test_end": _ms_to_iso(best_fold.get("test_end_ms")),
            "net_pnl": best_fold_net_pnl,
            "max_dd_pct": _f(best_fold.get("test_max_dd_pct")),
        },
        "worst_fold": {
            "fold": int(worst_fold.get("fold", worst_idx)),
            "test_start": _ms_to_iso(worst_fold.get("test_start_ms")),
            "test_end": _ms_to_iso(worst_fold.get("test_end_ms")),
            "net_pnl": worst_fold_net_pnl,
            "max_dd_pct": _f(worst_fold.get("test_max_dd_pct")),
        },
        "param_frequency": dict(sorted(pf.items(), key=lambda kv: kv[1], reverse=True)),
        "execution_stats": _collect_execution_stats(folds),
    }
    return out


def _to_markdown(report: dict[str, Any]) -> str:
    s = report.get("summary") or {}
    st = report.get("stability") or {}
    b = report.get("best_fold") or {}
    w = report.get("worst_fold") or {}
    pf = report.get("param_frequency") or {}
    ex = report.get("execution_stats") or {}

    top_param = next(iter(pf.items()), ("n/a", 0))
    lines = [
        "# Walk-Forward Report",
        "",
        "## Summary",
        f"- Fold count: {s.get('fold_count')}",
        f"- Total net PnL: {s.get('total_net_pnl')}",
        f"- Final equity: {s.get('final_equity')}",
        f"- Avg win rate: {s.get('avg_win_rate')}",
        f"- Avg trade count: {s.get('avg_trade_count')}",
        "",
        "## Stability",
        f"- Worst fold PnL: {st.get('worst_fold_net_pnl')}",
        f"- Std test net PnL: {st.get('std_test_net_pnl')}",
        f"- Avg drawdown pct: {s.get('avg_max_dd_pct')}",
        f"- Worst fold max DD: {st.get('worst_fold_max_dd')}",
        "",
        "## Best / Worst Fold",
        f"- Best fold: #{b.get('fold')} ({b.get('test_start')} -> {b.get('test_end')}), net={b.get('net_pnl')}, dd={b.get('max_dd_pct')}",
        f"- Worst fold: #{w.get('fold')} ({w.get('test_start')} -> {w.get('test_end')}), net={w.get('net_pnl')}, dd={w.get('max_dd_pct')}",
        "",
        "## Parameter Drift",
        f"- Most frequent params: `{top_param[0]}` ({top_param[1]} folds)",
    ]
    if ex:
        lines.extend(
            [
                "",
                "## Execution Stats",
                f"- Avg entry slippage (bps): {ex.get('avg_entry_slippage_bps')}",
                f"- Avg exit slippage (bps): {ex.get('avg_exit_slippage_bps')}",
                f"- Avg fee (bps): {ex.get('avg_fee_bps')}",
            ]
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    p = argparse.ArgumentParser(description="Analyze walk-forward JSON and generate report files")
    p.add_argument("--in", dest="in_path", required=True, help="Path to walkforward JSON output")
    args = p.parse_args()

    in_path = Path(args.in_path).resolve()
    raw = in_path.read_bytes()
    payload = None
    for enc in ("utf-8-sig", "utf-16", "utf-16-le", "utf-16-be"):
        try:
            payload = json.loads(raw.decode(enc))
            break
        except Exception:
            continue
    if payload is None:
        raise RuntimeError("failed_to_decode_input_json")

    report = analyze(payload)
    out_json = in_path.parent / "report.json"
    out_md = in_path.parent / "report.md"
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    out_md.write_text(_to_markdown(report), encoding="utf-8")
    print(json.dumps({"report_json": str(out_json), "report_md": str(out_md)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
