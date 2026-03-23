from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .edge_diagnostics import (
    DiagnosticsConstraints,
    FeatureRow,
    PolicyRecipe,
    _calc_stats,
    compute_segment_stats,
    extract_feature_rows,
    generate_rules,
    policy_recipe_from_dict,
    policy_recipe_to_dict,
)
from .overlay_policy import DataDrivenOverlayPolicy


def _extract_trades_from_payload(payload: Any) -> list[Any]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return list(payload)
    if isinstance(payload, dict):
        if isinstance(payload.get("trades"), list):
            return list(payload.get("trades") or [])
        folds = payload.get("folds")
        if isinstance(folds, list):
            out: list[Any] = []
            for fold in folds:
                if not isinstance(fold, dict):
                    continue
                for key in ("test_trades", "trades"):
                    arr = fold.get(key)
                    if isinstance(arr, list):
                        out.extend(arr)
            return out
        for key in ("overlay", "base"):
            node = payload.get(key)
            if isinstance(node, dict) and isinstance(node.get("trades"), list):
                return list(node.get("trades") or [])
    return []


def _split_rows(rows: list[FeatureRow], train_ratio: float) -> tuple[list[FeatureRow], list[FeatureRow]]:
    if not rows:
        return [], []
    rows_sorted = sorted(rows, key=lambda r: int(r.ts_ms))
    cut = int(len(rows_sorted) * float(train_ratio))
    cut = max(1, min(len(rows_sorted) - 1, cut)) if len(rows_sorted) > 1 else 1
    return rows_sorted[:cut], rows_sorted[cut:]


def generate_data_driven_policy(
    trades_or_payload: Any,
    split_config: dict[str, Any] | None = None,
    constraints: dict[str, Any] | DiagnosticsConstraints | None = None,
) -> tuple[DataDrivenOverlayPolicy, PolicyRecipe]:
    trades = _extract_trades_from_payload(trades_or_payload)
    rows_all = extract_feature_rows(trades)
    split = dict(split_config or {})
    train_ratio = float(split.get("train_ratio", 0.7))
    train_ratio = max(0.1, min(0.9, train_ratio))
    rows_train, rows_val = _split_rows(rows_all, train_ratio)

    if isinstance(constraints, DiagnosticsConstraints):
        cfg = constraints
        extra_constraints: dict[str, Any] = {}
    else:
        extra_constraints = dict(constraints or {})
        cfg = DiagnosticsConstraints(
            min_samples_per_rule=int(extra_constraints.get("min_samples_per_rule", 30)),
            objective_lambda=float(extra_constraints.get("objective_lambda", 100.0)),
            top_k=int(extra_constraints.get("top_k", 8)),
            min_objective_delta_per_trade=float(extra_constraints.get("min_objective_delta_per_trade", 0.0)),
        )

    train_stats = compute_segment_stats(rows_train, objective_lambda=cfg.objective_lambda)
    val_stats = compute_segment_stats(rows_val, objective_lambda=cfg.objective_lambda)
    val_all_stats = _calc_stats(rows_val, cfg.objective_lambda)

    merged_constraints: dict[str, Any] = {
        **extra_constraints,
        "min_samples_per_rule": cfg.min_samples_per_rule,
        "objective_lambda": cfg.objective_lambda,
        "top_k": cfg.top_k,
        "min_objective_delta_per_trade": cfg.min_objective_delta_per_trade,
        "validation_stats": val_stats,
        "baseline_validation_stats": val_all_stats,
        "rows_all": rows_all,
        "validation_rows": rows_val,
    }
    recipe = generate_rules(train_stats, constraints=merged_constraints)
    policy = DataDrivenOverlayPolicy(recipe=recipe)
    return policy, recipe


def generate_data_driven_policy_from_json(
    in_path: str,
    *,
    split_config: dict[str, Any] | None = None,
    constraints: dict[str, Any] | DiagnosticsConstraints | None = None,
) -> tuple[DataDrivenOverlayPolicy, PolicyRecipe]:
    payload = json.loads(Path(in_path).read_text(encoding="utf-8"))
    return generate_data_driven_policy(payload, split_config=split_config, constraints=constraints)


def write_policy_recipe(path: str, recipe: PolicyRecipe) -> None:
    Path(path).write_text(json.dumps(policy_recipe_to_dict(recipe), indent=2), encoding="utf-8")


def load_policy_recipe_dict(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_policy_recipe(path: str) -> PolicyRecipe:
    return policy_recipe_from_dict(load_policy_recipe_dict(path))
