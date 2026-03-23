from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .monte_carlo_risk import MCRiskConfig, run_monte_carlo


def _load_json_payload(path: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    payload = None
    for enc in ("utf-8-sig", "utf-16", "utf-16-le", "utf-16-be"):
        try:
            payload = json.loads(raw.decode(enc))
            break
        except Exception:
            continue
    if not isinstance(payload, dict):
        raise RuntimeError("failed_to_decode_input_json")
    return payload


def _extract_trades_and_equity(payload: dict[str, Any]) -> tuple[list[Any], float]:
    if isinstance(payload.get("trades"), list):
        return list(payload.get("trades") or []), float(payload.get("initial_equity") or 0.0)

    # true-overlay payload fallback: prefer overlay run, then base run
    for key in ("overlay", "base"):
        node = payload.get(key)
        if isinstance(node, dict) and isinstance(node.get("trades"), list):
            return list(node.get("trades") or []), float(node.get("initial_equity") or 0.0)

    raise RuntimeError("invalid_replay_json_missing_trades")


def main() -> int:
    p = argparse.ArgumentParser(description="Offline Monte Carlo risk evaluator for replay output JSON")
    p.add_argument("--input", required=True, help="Path to replay output JSON")
    p.add_argument("--out", default=None, help="Optional output path for MC report JSON")
    p.add_argument("--mode", choices=["pnl", "r"], default="pnl")
    p.add_argument("--n-sims", type=int, default=5000)
    p.add_argument("--horizon-trades", type=int, default=None)
    p.add_argument("--block-size", type=int, default=1)
    p.add_argument("--ruin-floor-pct", type=float, default=0.5)
    p.add_argument("--cost-mult", type=float, default=1.0)
    p.add_argument("--pnl-shrink", type=float, default=1.0)
    p.add_argument("--loss-tail-mult", type=float, default=1.0)
    p.add_argument("--seed", type=int, default=7)
    args = p.parse_args()

    payload = _load_json_payload(Path(args.input).resolve())
    trades, initial_equity = _extract_trades_and_equity(payload)
    cfg = MCRiskConfig(
        n_sims=int(args.n_sims),
        horizon_trades=int(args.horizon_trades) if args.horizon_trades is not None else None,
        block_size=int(args.block_size),
        ruin_floor_pct=float(args.ruin_floor_pct),
        cost_mult=float(args.cost_mult),
        pnl_shrink=float(args.pnl_shrink),
        loss_tail_mult=float(args.loss_tail_mult),
        seed=int(args.seed),
    )
    result = run_monte_carlo(trades, float(initial_equity), cfg, mode=str(args.mode))
    out = asdict(result)
    if args.out:
        Path(str(args.out)).write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
