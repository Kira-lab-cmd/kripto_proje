# File: backend/replay.py
from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .edge_stability_engine import evaluate_edge_stability
from .edge_diagnostics import policy_recipe_to_dict
from .execution_model import DefaultExecutionModel, ExecutionModel
from .monte_carlo_risk import MCRiskConfig, run_monte_carlo
from .overlay_policy import OverlayPolicy, build_overlay_policy, overlay_policy_from_recipe
from .policy_generator import generate_data_driven_policy
from .portfolio_risk import PortfolioRiskEngine, RiskLimits
from .research_store import ResearchStore
from .strategy import TradingStrategy
from .replay_engine import ReplayEngine, _parse_date

UTC = timezone.utc


def _to_payload(res) -> dict:
    trades: list[dict] = []
    for t in res.trades:
        trades.append(asdict(t))
    return {
        "symbols": res.symbols,
        "start_ms": res.start_ms,
        "end_ms": res.end_ms,
        "initial_equity": res.initial_equity,
        "final_equity": res.final_equity,
        "metrics": res.metrics,
        "strategy_config": res.strategy_config or {},
        "buckets": res.buckets or {},
        "trades": trades,
    }


def _execution_model_enabled(args) -> bool:
    legacy_mode = str(getattr(args, "execution_model", "realistic") or "realistic").strip().lower()
    if bool(args.no_execution_model):
        return False
    if legacy_mode == "none":
        return False
    return True


def _build_execution_model(args) -> ExecutionModel | None:
    if not _execution_model_enabled(args):
        return None

    taker_fee_bps = float(args.taker_fee_bps)
    if getattr(args, "fee_bps_taker", None) is not None:
        taker_fee_bps = float(args.fee_bps_taker)

    base_slippage_bps = float(args.base_slippage_bps)
    if getattr(args, "slippage_bps", None) is not None:
        base_slippage_bps = float(args.slippage_bps)

    max_slippage_bps: float | None
    if args.max_slippage_bps is None:
        max_slippage_bps = None
    else:
        max_slippage_bps = float(args.max_slippage_bps)
        if max_slippage_bps < 0:
            max_slippage_bps = None

    return DefaultExecutionModel(
        taker_fee_bps=taker_fee_bps,
        base_slippage_bps=base_slippage_bps,
        slippage_atr_k=float(args.slippage_atr_k),
        max_slippage_bps=max_slippage_bps,
    )


# Backward-compatible helper name used by older tests/callers.
def _build_execution_engine(args) -> ExecutionModel | None:
    return _build_execution_model(args)


def _build_overlay_policy(args) -> tuple[OverlayPolicy | None, bool]:
    policy, enabled, _, _ = _build_overlay_policy_with_recipe(args)
    return policy, enabled


def _load_recipe_json(path: str) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("overlay_recipe_invalid_root")
    return payload


def _build_overlay_policy_with_recipe(args) -> tuple[OverlayPolicy | None, bool, dict[str, Any] | None, str]:
    mode_raw = str(getattr(args, "overlay", "none") or "none").strip().lower()
    mode = "data" if mode_raw == "data_driven" else mode_raw
    if mode not in {"none", "atr_regime", "data"}:
        mode = "none"
    enabled_flag = bool(getattr(args, "enable_overlay", False))

    if mode == "none":
        return None, False, None, "none"

    if mode == "atr_regime":
        policy = build_overlay_policy(
            "atr_regime",
            target_atr=float(args.target_atr),
            atr_min=float(args.atr_min),
            atr_max=float(args.atr_max),
            trend_scalar=float(args.trend_scalar),
            soft_trend_scalar=float(args.soft_trend_scalar),
            chop_scalar=float(args.chop_scalar),
            clamp_lo=float(args.clamp_lo),
            clamp_hi=float(args.clamp_hi),
        )
        return policy, True if (enabled_flag or mode != "none") else False, None, "atr_regime"

    recipe_path = getattr(args, "overlay_recipe", None) or getattr(args, "overlay_policy_path", None)
    if not recipe_path:
        raise ValueError("overlay_recipe_required_for_data_overlay")
    recipe = _load_recipe_json(str(recipe_path))
    policy = overlay_policy_from_recipe(recipe)
    return policy, True, recipe, "data"


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


def _extract_mc_source(payload: dict) -> tuple[list[dict], float]:
    if isinstance(payload.get("trades"), list):
        return list(payload.get("trades") or []), float(payload.get("initial_equity") or 0.0)
    for key in ("overlay", "base"):
        node = payload.get(key)
        if isinstance(node, dict) and isinstance(node.get("trades"), list):
            return list(node.get("trades") or []), float(node.get("initial_equity") or 0.0)
    return [], 0.0


def main() -> int:
    p = argparse.ArgumentParser(description="Deterministic bar-by-bar replay using research.db + strategy.get_signal()")
    p.add_argument("--db", default=str((__import__("pathlib").Path(__file__).resolve().parent / "research.db")), help="Path to research.db")
    p.add_argument("--symbols", default="BTC/USDT,ETH/USDT", help="Comma-separated symbols")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD or ISO)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD or ISO)")
    p.add_argument("--equity", type=float, default=200.0, help="Initial equity (USDT)")
    p.add_argument("--sentiment", type=float, default=0.0, help="Sentiment score (constant for replay)")
    p.add_argument("--true-overlay", action="store_true", help="Run base + overlay replay and compare")
    p.add_argument("--target-atr", type=float, default=0.004)
    p.add_argument("--atr-min", type=float, default=0.0025)
    p.add_argument("--atr-max", type=float, default=0.010)
    p.add_argument("--trend-scalar", type=float, default=1.10)
    p.add_argument("--soft-trend-scalar", type=float, default=0.90)
    p.add_argument("--chop-scalar", type=float, default=0.65)
    p.add_argument("--clamp-lo", type=float, default=0.50)
    p.add_argument("--clamp-hi", type=float, default=1.50)
    p.add_argument("--overlay", choices=["none", "atr_regime", "data", "data_driven"], default="none")
    p.add_argument("--overlay-recipe", default=None, help="Path to data-driven overlay recipe JSON")
    p.add_argument("--overlay-policy-path", default=None, help="Path to data-driven overlay policy JSON")
    p.add_argument("--enable-overlay", action="store_true")
    p.add_argument("--enable-portfolio-risk", action="store_true")
    p.add_argument("--max-gross-exposure-pct", type=float, default=1.50)
    p.add_argument("--max-net-exposure-pct", type=float, default=1.00)
    p.add_argument("--max-per-symbol-exposure-pct", type=float, default=0.60)
    p.add_argument("--max-concurrent-positions", type=int, default=2)
    p.add_argument("--generate-policy", action="store_true", help="Generate data-driven policy recipe from replay trades")
    p.add_argument("--policy-out", default=None, help="Optional output path for generated policy recipe JSON")
    p.add_argument("--mc-risk", action="store_true", help="Attach Monte Carlo risk report to replay payload")
    p.add_argument("--mc-mode", choices=["pnl", "r"], default="pnl")
    p.add_argument("--mc-n-sims", type=int, default=5000)
    p.add_argument("--mc-horizon-trades", type=int, default=None)
    p.add_argument("--mc-block-size", type=int, default=1)
    p.add_argument("--mc-ruin-floor-pct", type=float, default=0.5)
    p.add_argument("--mc-cost-mult", type=float, default=1.0)
    p.add_argument("--mc-pnl-shrink", type=float, default=1.0)
    p.add_argument("--mc-loss-tail-mult", type=float, default=1.0)
    p.add_argument("--mc-seed", type=int, default=7)
    p.add_argument("--edge-stability", action="store_true", help="Attach edge stability summary to replay payload")
    p.add_argument("--dashboard", action="store_true", help="Attach compact research dashboard to replay payload")
    p.add_argument("--dump-policy", default=None, help="Dump recipe JSON found in payload (overlay/policy_recipe)")
    p.add_argument("--taker-fee-bps", type=float, default=10.0)
    p.add_argument("--base-slippage-bps", type=float, default=2.0)
    p.add_argument("--slippage-atr-k", type=float, default=1.0)
    p.add_argument("--max-slippage-bps", type=float, default=15.0)
    p.add_argument("--no-execution-model", action="store_true", help="Disable execution model and use legacy replay fill behavior")

    # Legacy flags kept for compatibility with existing scripts.
    p.add_argument("--execution-model", choices=["none", "basic", "realistic"], default="realistic", help=argparse.SUPPRESS)
    p.add_argument("--fee-bps-maker", type=float, default=None, help=argparse.SUPPRESS)
    p.add_argument("--fee-bps-taker", type=float, default=None, help=argparse.SUPPRESS)
    p.add_argument("--slippage-bps", type=float, default=None, help=argparse.SUPPRESS)
    p.add_argument("--seed", type=int, default=7, help=argparse.SUPPRESS)
    args = p.parse_args()

    store = ResearchStore(args.db)
    store.init_schema()

    strategy = TradingStrategy()
    use_execution_model = _execution_model_enabled(args)
    execution_model = _build_execution_model(args)
    overlay_policy, overlay_enabled, loaded_overlay_recipe, overlay_mode = _build_overlay_policy_with_recipe(args)
    portfolio_risk_engine = _build_portfolio_risk_engine(args)

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    start = _parse_date(args.start)
    end = _parse_date(args.end)

    if args.true_overlay:
        if overlay_policy is None:
            overlay_policy, overlay_enabled, loaded_overlay_recipe, overlay_mode = _build_overlay_policy_with_recipe(
                argparse.Namespace(
                    **{
                        **vars(args),
                        "overlay": "atr_regime",
                        "enable_overlay": True,
                    }
                )
            )
        base_eng = ReplayEngine(
            store,
            strategy,
            execution_model=execution_model,
            enable_execution_model=use_execution_model,
            overlay_policy=None,
            enable_overlay=False,
            portfolio_risk_engine=portfolio_risk_engine,
            enable_portfolio_risk=bool(portfolio_risk_engine is not None),
        )
        base_res = base_eng.run(symbols, start=start, end=end, initial_equity=args.equity, sentiment_score=args.sentiment)
        ov_eng = ReplayEngine(
            store,
            strategy,
            overlay_policy=overlay_policy,
            enable_overlay=overlay_enabled,
            execution_model=execution_model,
            enable_execution_model=use_execution_model,
            portfolio_risk_engine=portfolio_risk_engine,
            enable_portfolio_risk=bool(portfolio_risk_engine is not None),
        )
        ov_res = ov_eng.run(symbols, start=start, end=end, initial_equity=args.equity, sentiment_score=args.sentiment)

        base_payload = _to_payload(base_res)
        overlay_payload = _to_payload(ov_res)

        base_net = float(base_res.metrics.get("net_pnl") or 0.0)
        ov_net = float(ov_res.metrics.get("net_pnl") or 0.0)
        delta = ov_net - base_net
        payload = {
            "base": base_payload,
            "overlay": overlay_payload,
            "comparison": {
                "base_trade_count": int(base_res.metrics.get("trade_count") or 0),
                "overlay_trade_count": int(ov_res.metrics.get("trade_count") or 0),
                "base_net_pnl": base_net,
                "overlay_net_pnl": ov_net,
                "net_pnl_delta": delta,
                "base_win_rate": float(base_res.metrics.get("win_rate") or 0.0),
                "overlay_win_rate": float(ov_res.metrics.get("win_rate") or 0.0),
                "win_rate_delta": float(ov_res.metrics.get("win_rate") or 0.0) - float(base_res.metrics.get("win_rate") or 0.0),
                "recommendation": "overlay_better" if delta > 0 else "base_better_or_equal",
            },
        }
        if payload["base"]["trades"]:
            assert "regime" in payload["base"]["trades"][0], payload["base"]["trades"][0].keys()
        if payload["overlay"]["trades"]:
            assert "regime" in payload["overlay"]["trades"][0], payload["overlay"]["trades"][0].keys()
    else:
        eng = ReplayEngine(
            store,
            strategy,
            overlay_policy=overlay_policy,
            enable_overlay=overlay_enabled,
            execution_model=execution_model,
            enable_execution_model=use_execution_model,
            portfolio_risk_engine=portfolio_risk_engine,
            enable_portfolio_risk=bool(portfolio_risk_engine is not None),
        )
        base_res = eng.run(symbols, start=start, end=end, initial_equity=args.equity, sentiment_score=args.sentiment)
        payload = _to_payload(base_res)
        if payload["trades"]:
            assert "regime" in payload["trades"][0], payload["trades"][0].keys()

    overlay_flag_used = bool(getattr(args, "enable_overlay", False)) or str(getattr(args, "overlay", "none")).strip().lower() != "none"
    if overlay_flag_used:
        overlay_meta = {"enabled": bool(overlay_enabled), "type": str(overlay_mode)}
        if args.true_overlay:
            payload["overlay_meta"] = overlay_meta
        else:
            payload["overlay"] = overlay_meta
    if isinstance(loaded_overlay_recipe, dict):
        payload["overlay_recipe"] = loaded_overlay_recipe

    # HOOK: POLICY_GENERATION (post-run)
    if bool(getattr(args, "generate_policy", False)):
        source_res = ov_res if args.true_overlay else base_res
        source_trades = [asdict(t) for t in list(source_res.trades)]
        _, recipe = generate_data_driven_policy(
            source_trades,
            constraints={"objective_lambda": 100.0, "min_samples_per_rule": 30, "top_k": 8},
        )
        recipe_payload = policy_recipe_to_dict(recipe)
        payload["policy_recipe"] = recipe_payload
        if args.policy_out:
            Path(str(args.policy_out)).write_text(json.dumps(recipe_payload, indent=2), encoding="utf-8")

    if bool(getattr(args, "mc_risk", False)):
        trades_mc, equity_mc = _extract_mc_source(payload)
        mc_cfg = MCRiskConfig(
            n_sims=int(args.mc_n_sims),
            horizon_trades=int(args.mc_horizon_trades) if args.mc_horizon_trades is not None else None,
            block_size=int(args.mc_block_size),
            ruin_floor_pct=float(args.mc_ruin_floor_pct),
            cost_mult=float(args.mc_cost_mult),
            pnl_shrink=float(args.mc_pnl_shrink),
            loss_tail_mult=float(args.mc_loss_tail_mult),
            seed=int(args.mc_seed),
        )
        payload["mc_risk"] = asdict(run_monte_carlo(trades_mc, float(equity_mc), mc_cfg, mode=str(args.mc_mode)))
    if bool(getattr(args, "edge_stability", False)):
        payload["edge_stability"] = evaluate_edge_stability(payload)
    if bool(getattr(args, "dashboard", False)):
        try:
            from .research_dashboard import build_research_dashboard

            payload["dashboard"] = build_research_dashboard(
                replay_payload=payload,
                walkforward_payload=None,
                include_edge_stability=True,
            )
        except Exception as exc:
            payload["dashboard"] = {"error": "dashboard_unavailable", "detail": str(exc)}
    if args.dump_policy:
        recipe_to_dump = None
        for key in ("generated_policy", "policy_recipe", "overlay_recipe"):
            if isinstance(payload.get(key), dict):
                recipe_to_dump = payload.get(key)
                break
        if recipe_to_dump is not None:
            Path(str(args.dump_policy)).write_text(json.dumps(recipe_to_dump, indent=2), encoding="utf-8")

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
