from __future__ import annotations

import argparse
import itertools
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .edge_stability_engine import evaluate_edge_stability
from .edge_diagnostics import policy_recipe_to_dict
from .execution_model import ExecutionEngine
from .overlay_policy import build_overlay_policy, overlay_policy_from_recipe
from .policy_generator import generate_data_driven_policy
from .portfolio_allocation import allocate_portfolio
from .portfolio_risk import PortfolioRiskEngine, RiskLimits
from .replay_engine import ReplayEngine, _parse_date
from .research_store import ResearchStore
from .strategy import TradingStrategy

UTC = timezone.utc


def _dt_to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def _parse_grid_float(raw: str) -> list[float]:
    out: list[float] = []
    for part in (raw or "").split(","):
        s = part.strip()
        if not s:
            continue
        out.append(float(s))
    if not out:
        raise ValueError("empty float grid")
    return out


def _build_execution_engine(args) -> ExecutionEngine | None:
    mode = str(args.execution_model or "none").strip().lower()
    if mode == "none":
        return None
    if mode == "realistic":
        return ExecutionEngine.from_realistic(
            fee_bps_maker=float(args.fee_bps_maker),
            fee_bps_taker=float(args.fee_bps_taker),
            slippage_bps=float(args.slippage_bps),
            seed=int(args.seed),
        )
    return ExecutionEngine.from_basic(
        fee_bps_maker=float(args.fee_bps_maker),
        fee_bps_taker=float(args.fee_bps_taker),
        slippage_bps=float(args.slippage_bps),
        seed=int(args.seed),
    )


def _build_overlay_policy(args):
    mode = str(getattr(args, "overlay", "none") or "none").strip().lower()
    enabled = bool(getattr(args, "use_overlay", False)) or bool(getattr(args, "enable_overlay", False)) or mode != "none"
    if not enabled:
        return None
    if mode == "none":
        mode = "atr_regime"
    cfg: dict[str, Any] = {}
    if mode == "atr_regime" and args.overlay_config:
        with open(str(args.overlay_config), "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, dict):
                cfg = loaded

    def _ov(name: str, default: float) -> float:
        v = getattr(args, name, None)
        if v is not None:
            return float(v)
        if name in cfg:
            return float(cfg[name])
        return float(default)

    return build_overlay_policy(
        mode,
        recipe_path=(getattr(args, "overlay_recipe", None) or getattr(args, "overlay_policy_path", None)),
        target_atr=_ov("target_atr", 0.004),
        atr_min=_ov("atr_min", 0.0025),
        atr_max=_ov("atr_max", 0.010),
        trend_scalar=_ov("trend_scalar", 1.10),
        soft_trend_scalar=_ov("soft_trend_scalar", 0.90),
        chop_scalar=_ov("chop_scalar", 0.65),
        clamp_lo=_ov("clamp_lo", 0.50),
        clamp_hi=_ov("clamp_hi", 1.50),
    )


def _build_portfolio_risk_engine(args) -> PortfolioRiskEngine | None:
    if not bool(getattr(args, "enable_portfolio_risk", False)):
        return None
    limits = RiskLimits(
        max_gross_exposure_pct=float(args.max_gross_exposure_pct),
        max_net_exposure_pct=float(args.max_net_exposure_pct),
        max_per_symbol_exposure_pct=float(args.max_per_symbol_exposure_pct),
        max_concurrent_positions=int(args.max_concurrent_positions),
    )
    return PortfolioRiskEngine(limits=limits)


def _load_recipe_json(path: str) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("policy_in_invalid_root")
    return payload


def _make_strategy(params: dict[str, float]) -> TradingStrategy:
    strat = TradingStrategy()
    strat.buy_threshold = float(params["buy_th"])
    strat.sell_threshold = float(params["sell_th"])
    strat.atr_sl_mult = float(params["atr_sl_mult"])
    strat.atr_tp_mult = float(params["atr_tp_mult"])
    return strat


def _max_drawdown_pct(initial_equity: float, trades: list[Any]) -> float:
    eq = float(initial_equity)
    peak = eq
    max_dd = 0.0
    for t in sorted(trades, key=lambda x: int(getattr(x, "exit_ts_ms", 0))):
        eq += float(getattr(t, "realized_pnl", 0.0) or 0.0)
        if eq > peak:
            peak = eq
        if peak > 0:
            dd = (peak - eq) / peak
            if dd > max_dd:
                max_dd = dd
    return float(max_dd)


def _objective(res, lam: float) -> tuple[float, float]:
    net = float((res.metrics or {}).get("net_pnl") or 0.0)
    max_dd = _max_drawdown_pct(float(res.initial_equity), list(res.trades))
    return float(net - lam * max_dd), float(max_dd)


def main() -> int:
    p = argparse.ArgumentParser(description="Walk-forward replay runner (cost-aware, optional overlay)")
    p.add_argument("--db", default=str((__import__("pathlib").Path(__file__).resolve().parent / "research.db")), help="Path to research.db")
    p.add_argument("--symbols", default="BTC/USDT,ETH/USDT", help="Comma-separated symbols")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD or ISO)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD or ISO)")
    p.add_argument("--equity", type=float, default=200.0, help="Initial equity (USDT)")
    p.add_argument("--sentiment", type=float, default=0.0, help="Sentiment score (constant for replay)")
    p.add_argument("--train-days", type=int, default=60)
    p.add_argument("--test-days", type=int, default=14)
    p.add_argument("--step-days", type=int, default=14)
    p.add_argument("--objective-lambda", type=float, default=100.0, help="Objective = net_pnl - lambda*maxDD_pct")
    p.add_argument("--buy-th-grid", default="2.0,2.5,3.0")
    p.add_argument("--sell-th-grid", default="-2.0,-2.5,-3.0")
    p.add_argument("--atr-sl-grid", default="1.5,2.0,2.5")
    p.add_argument("--atr-tp-grid", default="2.5,3.0,3.5")
    p.add_argument("--execution-model", choices=["none", "basic", "realistic"], default="none")
    p.add_argument("--fee-bps-maker", type=float, default=10.0)
    p.add_argument("--fee-bps-taker", type=float, default=10.0)
    p.add_argument("--slippage-bps", type=float, default=2.0)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--use-overlay", action="store_true", help="Enable AtrRegime overlay policy in walk-forward runs")
    p.add_argument("--enable-overlay", action="store_true")
    p.add_argument("--overlay", choices=["none", "atr_regime", "data", "data_driven"], default="none")
    p.add_argument("--overlay-recipe", default=None, help="Path to data-driven overlay recipe JSON")
    p.add_argument("--overlay-config", default=None, help="Optional JSON config for overlay parameters")
    p.add_argument("--overlay-policy-path", default=None, help="Path to data-driven overlay policy JSON")
    p.add_argument("--use-data-overlay", action="store_true", help="Use overlay policy from --policy-in recipe JSON")
    p.add_argument("--policy-in", default=None, help="Input recipe JSON path for --use-data-overlay")
    p.add_argument("--enable-portfolio-risk", action="store_true")
    p.add_argument("--max-gross-exposure-pct", type=float, default=1.50)
    p.add_argument("--max-net-exposure-pct", type=float, default=1.00)
    p.add_argument("--max-per-symbol-exposure-pct", type=float, default=0.60)
    p.add_argument("--max-concurrent-positions", type=int, default=2)
    p.add_argument("--generate-policy", action="store_true", help="Generate data-driven policy recipe from test fold trades")
    p.add_argument("--policy-out", default=None, help="Optional output path for generated policy recipe JSON")
    p.add_argument("--edge-stability", action="store_true", help="Attach edge stability summary to walk-forward output")
    p.add_argument("--allocate-portfolio", action="store_true", help="Attach portfolio allocation summary to walk-forward output")
    p.add_argument("--dashboard", action="store_true", help="Attach research dashboard summary to output")
    p.add_argument("--allocation", action="store_true", help="Attach allocation summary to output")
    p.add_argument("--allocation-out", default=None, help="Optional output path for allocation JSON")
    p.add_argument("--target-atr", type=float, default=None)
    p.add_argument("--atr-min", type=float, default=None)
    p.add_argument("--atr-max", type=float, default=None)
    p.add_argument("--trend-scalar", type=float, default=None)
    p.add_argument("--soft-trend-scalar", type=float, default=None)
    p.add_argument("--chop-scalar", type=float, default=None)
    p.add_argument("--clamp-lo", type=float, default=None)
    p.add_argument("--clamp-hi", type=float, default=None)
    p.add_argument("--fail-on-leak", action="store_true")
    args = p.parse_args()

    store = ResearchStore(args.db)
    store.init_schema()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    start = _parse_date(args.start)
    end = _parse_date(args.end)

    buy_grid = _parse_grid_float(args.buy_th_grid)
    sell_grid = _parse_grid_float(args.sell_th_grid)
    sl_grid = _parse_grid_float(args.atr_sl_grid)
    tp_grid = _parse_grid_float(args.atr_tp_grid)
    param_grid = [
        {
            "buy_th": b,
            "sell_th": s,
            "atr_sl_mult": slm,
            "atr_tp_mult": tpm,
        }
        for b, s, slm, tpm in itertools.product(buy_grid, sell_grid, sl_grid, tp_grid)
    ]

    loaded_policy_in: dict[str, Any] | None = None
    if bool(getattr(args, "use_data_overlay", False)):
        if not args.policy_in:
            raise ValueError("policy_in_required_when_use_data_overlay")
        loaded_policy_in = _load_recipe_json(str(args.policy_in))
        overlay_policy = overlay_policy_from_recipe(loaded_policy_in)
    else:
        overlay_policy = _build_overlay_policy(args)
    portfolio_risk_engine = _build_portfolio_risk_engine(args)
    train_days = int(args.train_days)
    test_days = int(args.test_days)
    step_days = int(args.step_days)
    if train_days <= 0 or test_days <= 0 or step_days <= 0:
        raise ValueError("train/test/step days must be > 0")

    folds: list[dict[str, Any]] = []
    equity = float(args.equity)
    equity_points: list[dict[str, Any]] = [{"ts_ms": _dt_to_ms(start), "equity": equity}]
    leak_count_total = 0
    leak_examples: list[dict[str, Any]] = []
    policy_source_trades: list[dict[str, Any]] = []

    def _accumulate_leaks(res: Any) -> None:
        nonlocal leak_count_total
        ls = getattr(res, "leak_stats", None)
        if not isinstance(ls, dict):
            return
        leak_count_total += int(ls.get("leak_count") or 0)
        for ex in (ls.get("examples") or []):
            if len(leak_examples) >= 5:
                break
            if isinstance(ex, dict):
                leak_examples.append(ex)

    cursor = start
    fold_idx = 0
    while (cursor + timedelta(days=train_days + test_days)) <= end:
        train_start = cursor
        train_end = cursor + timedelta(days=train_days)
        test_start = train_end
        test_end = test_start + timedelta(days=test_days)

        best: dict[str, Any] | None = None
        for params in param_grid:
            strat = _make_strategy(params)
            eng = ReplayEngine(
                store,
                strat,
                overlay_policy=overlay_policy,
                execution_engine=_build_execution_engine(args),
                portfolio_risk_engine=portfolio_risk_engine,
                enable_portfolio_risk=bool(portfolio_risk_engine is not None),
            )
            train_res = eng.run(
                symbols,
                start=train_start,
                end=train_end,
                initial_equity=float(args.equity),
                sentiment_score=float(args.sentiment),
                fail_on_leak=bool(args.fail_on_leak),
            )
            _accumulate_leaks(train_res)
            obj, max_dd = _objective(train_res, float(args.objective_lambda))
            row = {
                "params": params,
                "objective": obj,
                "max_dd_pct": max_dd,
                "net_pnl": float(train_res.metrics.get("net_pnl") or 0.0),
                "metrics": train_res.metrics,
            }
            if best is None or row["objective"] > best["objective"]:
                best = row

        if best is None:
            break

        best_params = dict(best["params"])
        test_strat = _make_strategy(best_params)
        test_eng = ReplayEngine(
            store,
            test_strat,
            overlay_policy=overlay_policy,
            execution_engine=_build_execution_engine(args),
            portfolio_risk_engine=portfolio_risk_engine,
            enable_portfolio_risk=bool(portfolio_risk_engine is not None),
        )
        test_res = test_eng.run(
            symbols,
            start=test_start,
            end=test_end,
            initial_equity=float(equity),
            sentiment_score=float(args.sentiment),
            fail_on_leak=bool(args.fail_on_leak),
        )
        _accumulate_leaks(test_res)
        if bool(getattr(args, "generate_policy", False)):
            policy_source_trades.extend([asdict(t) for t in list(test_res.trades)])
        test_obj, test_max_dd = _objective(test_res, float(args.objective_lambda))
        test_net = float(test_res.metrics.get("net_pnl") or 0.0)
        equity += test_net
        equity_points.append({"fold": fold_idx, "ts_ms": _dt_to_ms(test_end), "equity": equity})

        folds.append(
            {
                "fold": fold_idx,
                "train_start_ms": _dt_to_ms(train_start),
                "train_end_ms": _dt_to_ms(train_end),
                "test_start_ms": _dt_to_ms(test_start),
                "test_end_ms": _dt_to_ms(test_end),
                "best_params": best_params,
                "train_objective": float(best["objective"]),
                "train_max_dd_pct": float(best["max_dd_pct"]),
                "train_metrics": best["metrics"],
                "test_objective": float(test_obj),
                "test_max_dd_pct": float(test_max_dd),
                "test_metrics": test_res.metrics,
            }
        )

        fold_idx += 1
        cursor = cursor + timedelta(days=step_days)

    if not folds:
        raise RuntimeError("no_walkforward_folds_generated")

    fold_count = len(folds)
    total_test_pnl = float(sum(float(f["test_metrics"].get("net_pnl") or 0.0) for f in folds))
    avg_test_win_rate = float(sum(float(f["test_metrics"].get("win_rate") or 0.0) for f in folds) / fold_count)
    avg_test_trade_count = float(sum(float(f["test_metrics"].get("trade_count") or 0.0) for f in folds) / fold_count)
    avg_test_max_dd = float(sum(float(f["test_max_dd_pct"]) for f in folds) / fold_count)

    out = {
        "config": {
            "symbols": symbols,
            "start_ms": _dt_to_ms(start),
            "end_ms": _dt_to_ms(end),
            "train_days": train_days,
            "test_days": test_days,
            "step_days": step_days,
            "objective_lambda": float(args.objective_lambda),
            "execution_model": str(args.execution_model),
            "seed": int(args.seed),
            "overlay_enabled": bool(args.use_overlay) or bool(args.enable_overlay) or str(args.overlay).lower() != "none",
            "overlay_mode": str(args.overlay),
            "overlay_recipe": str(args.overlay_recipe) if args.overlay_recipe else None,
            "overlay_config": str(args.overlay_config) if args.overlay_config else None,
            "overlay_policy_path": str(args.overlay_policy_path) if args.overlay_policy_path else None,
            "portfolio_risk_enabled": bool(portfolio_risk_engine is not None),
            "max_gross_exposure_pct": float(args.max_gross_exposure_pct),
            "max_net_exposure_pct": float(args.max_net_exposure_pct),
            "max_per_symbol_exposure_pct": float(args.max_per_symbol_exposure_pct),
            "max_concurrent_positions": int(args.max_concurrent_positions),
            "generate_policy": bool(args.generate_policy),
            "fail_on_leak": bool(args.fail_on_leak),
        },
        "leak_stats": {
            "leak_count": int(leak_count_total),
            "examples": leak_examples,
        },
        "folds": folds,
        "aggregated": {
            "fold_count": fold_count,
            "total_test_net_pnl": total_test_pnl,
            "avg_test_win_rate": avg_test_win_rate,
            "avg_test_trade_count": avg_test_trade_count,
            "avg_test_max_dd_pct": avg_test_max_dd,
        },
        "equity_curve_summary": {
            "initial_equity": float(args.equity),
            "final_equity": float(equity),
            "points": equity_points,
        },
    }
    # HOOK: POLICY_GENERATION (post-run)
    if bool(getattr(args, "generate_policy", False)):
        if policy_source_trades:
            _, recipe = generate_data_driven_policy(
                policy_source_trades,
                constraints={"objective_lambda": float(args.objective_lambda), "min_samples_per_rule": 30, "top_k": 8},
            )
            recipe_payload = policy_recipe_to_dict(recipe)
        else:
            recipe_payload = {
                "version": 1,
                "rules": [],
                "meta": {
                    "status": "insufficient_per_trade_data",
                    "reason": "walkforward_folds_do_not_include_per_trade_rows",
                },
            }
        out["generated_policy"] = recipe_payload
        # backward-compatible payload key retained
        out["policy_recipe"] = recipe_payload
        if args.policy_out:
            Path(str(args.policy_out)).write_text(json.dumps(recipe_payload, indent=2), encoding="utf-8")
    if bool(getattr(args, "edge_stability", False)):
        out["edge_stability"] = evaluate_edge_stability(out)
    if bool(getattr(args, "allocation", False) or getattr(args, "allocate_portfolio", False)):
        final_eq = float(((out.get("equity_curve_summary") or {}).get("final_equity")) or args.equity)
        alloc_raw = allocate_portfolio(out, total_equity=final_eq)
        weights: dict[str, float] = {}
        for row in list(alloc_raw.get("allocations") or []):
            if isinstance(row, dict):
                sym = str(row.get("symbol") or "")
                if sym:
                    weights[sym] = float(row.get("weight") or 0.0)
        alloc_payload = {
            "enabled": True,
            "weights": weights,
            "method": str(alloc_raw.get("method") or "unknown"),
            "inputs": {"total_equity": float(final_eq), "count": int(len(weights))},
            "raw": alloc_raw,
        }
        out["allocation"] = alloc_payload
        # backward-compatible key retained
        out["portfolio_allocation"] = alloc_raw
        if args.allocation_out:
            Path(str(args.allocation_out)).write_text(json.dumps(alloc_payload, indent=2), encoding="utf-8")
    if bool(getattr(args, "dashboard", False)):
        try:
            from .research_dashboard import build_research_dashboard

            out["dashboard"] = build_research_dashboard(
                replay_payload=None,
                walkforward_payload=out,
                include_edge_stability=True,
            )
        except Exception as exc:
            out["dashboard"] = {"error": "dashboard_unavailable", "detail": str(exc)}
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
