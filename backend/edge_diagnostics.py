from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

UTC = timezone.utc


@dataclass(frozen=True)
class FeatureRow:
    ts_ms: int
    symbol: str
    side: str
    regime: str
    adx: float | None
    er: float | None
    atr_pct: float | None
    trend_dir_1h: str
    score: float | None
    buy_th: float | None
    sell_th: float | None
    r_multiple: float
    realized_pnl: float
    fee_paid: float
    entry_slippage_bps: float | None
    exit_slippage_bps: float | None
    reason: str | None = None


@dataclass(frozen=True)
class SegmentStats:
    count: int
    win_rate: float
    avg_r: float
    avg_pnl: float
    pf: float | str
    max_dd_proxy: float
    net_pnl: float
    objective: float
    gross_profit: float
    gross_loss: float


@dataclass(frozen=True)
class Rule:
    name: str
    predicate: dict[str, Any]
    effect: dict[str, Any]
    note: str | None = None


@dataclass(frozen=True)
class PolicyRecipe:
    rules: list[Rule]
    meta: dict[str, Any]


@dataclass(frozen=True)
class DiagnosticsConstraints:
    min_samples_per_rule: int = 30
    objective_lambda: float = 100.0
    top_k: int = 8
    min_objective_delta_per_trade: float = 0.0


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_str(value: Any, default: str = "UNKNOWN") -> str:
    s = str(value).strip().upper() if value is not None else ""
    return s or default


def _row_get(trade: Any, key: str, default: Any = None) -> Any:
    if isinstance(trade, dict):
        return trade.get(key, default)
    return getattr(trade, key, default)


def extract_feature_rows(trades: list[Any]) -> list[FeatureRow]:
    rows: list[FeatureRow] = []
    for t in trades or []:
        side = _safe_str(_row_get(t, "side"), default="UNKNOWN")
        regime = _safe_str(_row_get(t, "regime"), default="UNKNOWN")
        trend = _safe_str(_row_get(t, "trend_dir_1h"), default="UNKNOWN")
        score = _safe_float(_row_get(t, "score"))
        buy_th = _safe_float(_row_get(t, "buy_th"))
        sell_th = _safe_float(_row_get(t, "sell_th"))
        realized = float(_safe_float(_row_get(t, "realized_pnl")) or 0.0)
        r_mult = float(_safe_float(_row_get(t, "r_multiple")) or 0.0)
        fee_paid = float(_safe_float(_row_get(t, "fee_paid")) or 0.0)
        rows.append(
            FeatureRow(
                ts_ms=int(_row_get(t, "entry_ts_ms", _row_get(t, "ts_ms", 0)) or 0),
                symbol=str(_row_get(t, "symbol", "") or ""),
                side=side,
                regime=regime,
                adx=_safe_float(_row_get(t, "adx")),
                er=_safe_float(_row_get(t, "er")),
                atr_pct=_safe_float(_row_get(t, "atr_pct")),
                trend_dir_1h=trend,
                score=score,
                buy_th=buy_th,
                sell_th=sell_th,
                r_multiple=r_mult,
                realized_pnl=realized,
                fee_paid=fee_paid,
                entry_slippage_bps=_safe_float(_row_get(t, "entry_slippage_bps")),
                exit_slippage_bps=_safe_float(_row_get(t, "exit_slippage_bps")),
                reason=_row_get(t, "reason"),
            )
        )
    return rows


def _atr_band(atr_pct: float | None) -> str:
    if atr_pct is None:
        return "unknown"
    v = float(atr_pct)
    if v < 0.003:
        return "lt_0.3pct"
    if v < 0.006:
        return "0.3_to_0.6pct"
    if v < 0.010:
        return "0.6_to_1.0pct"
    return "gte_1.0pct"


def _score_margin(row: FeatureRow) -> float | None:
    if row.score is None:
        return None
    if row.side == "BUY" and row.buy_th is not None:
        return float(row.score - row.buy_th)
    if row.side == "SELL" and row.sell_th is not None:
        return float(row.sell_th - row.score)
    return None


def _score_margin_bucket(row: FeatureRow) -> str:
    d = _score_margin(row)
    if d is None:
        return "unknown"
    if d < 0.25:
        return "lt_0.25"
    if d < 0.5:
        return "0.25_to_0.5"
    if d < 1.0:
        return "0.5_to_1.0"
    return "gte_1.0"


def _slippage_bucket(row: FeatureRow) -> str:
    slips = [x for x in [row.entry_slippage_bps, row.exit_slippage_bps] if x is not None]
    if not slips:
        return "unknown"
    s = float(sum(slips) / len(slips))
    if s < 2.0:
        return "lt_2bps"
    if s < 5.0:
        return "2_to_5bps"
    if s < 10.0:
        return "5_to_10bps"
    return "gte_10bps"


def _alignment(row: FeatureRow) -> str:
    trend = _safe_str(row.trend_dir_1h, default="UNKNOWN")
    side = _safe_str(row.side, default="UNKNOWN")
    if trend not in ("UP", "DOWN"):
        return "unknown"
    if (trend == "UP" and side == "BUY") or (trend == "DOWN" and side == "SELL"):
        return "aligned"
    return "counter"


def bucketize(rows: list[FeatureRow]) -> dict[str, list[FeatureRow]]:
    out: dict[str, list[FeatureRow]] = {}

    def _add(key: str, row: FeatureRow) -> None:
        out.setdefault(key, []).append(row)

    for row in rows:
        _add(f"regime={_safe_str(row.regime)}", row)
        _add(f"atr_band={_atr_band(row.atr_pct)}", row)
        _add(f"alignment={_alignment(row)}", row)
        _add(f"score_margin={_score_margin_bucket(row)}", row)
        _add(f"slippage_band={_slippage_bucket(row)}", row)
        _add(f"side={_safe_str(row.side)}", row)
    return out


def _max_dd_proxy(rows: list[FeatureRow]) -> float:
    eq = 0.0
    peak = 0.0
    max_dd = 0.0
    for row in sorted(rows, key=lambda r: int(r.ts_ms)):
        eq += float(row.realized_pnl)
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
    return float(max_dd)


def _calc_stats(rows: list[FeatureRow], objective_lambda: float) -> SegmentStats:
    count = len(rows)
    if count == 0:
        return SegmentStats(
            count=0,
            win_rate=0.0,
            avg_r=0.0,
            avg_pnl=0.0,
            pf=0.0,
            max_dd_proxy=0.0,
            net_pnl=0.0,
            objective=0.0,
            gross_profit=0.0,
            gross_loss=0.0,
        )
    wins = [r for r in rows if r.realized_pnl > 0]
    losses = [r for r in rows if r.realized_pnl < 0]
    gross_profit = float(sum(r.realized_pnl for r in wins))
    gross_loss = float(abs(sum(r.realized_pnl for r in losses)))
    pf: float | str
    if gross_loss > 0:
        pf = float(gross_profit / gross_loss)
    elif gross_profit > 0:
        pf = "inf"
    else:
        pf = 0.0
    net_pnl = float(sum(r.realized_pnl for r in rows))
    avg_pnl = float(net_pnl / count)
    avg_r = float(sum(float(r.r_multiple) for r in rows) / count)
    dd = _max_dd_proxy(rows)
    objective = float(net_pnl - float(objective_lambda) * dd)
    return SegmentStats(
        count=int(count),
        win_rate=float(len(wins) / count),
        avg_r=avg_r,
        avg_pnl=avg_pnl,
        pf=pf,
        max_dd_proxy=float(dd),
        net_pnl=float(net_pnl),
        objective=float(objective),
        gross_profit=float(gross_profit),
        gross_loss=float(gross_loss),
    )


def compute_segment_stats(rows: list[FeatureRow], objective_lambda: float = 100.0) -> dict[str, SegmentStats]:
    buckets = bucketize(rows)
    return {k: _calc_stats(v, objective_lambda=float(objective_lambda)) for k, v in buckets.items()}


def _rule_for_bucket(bucket_key: str, *, negative: bool, stats: SegmentStats) -> Rule:
    field, value = bucket_key.split("=", 1)
    predicate: dict[str, Any] = {}
    effect: dict[str, Any] = {}

    if field == "regime":
        predicate["regime"] = [value]
    elif field == "side":
        predicate["side"] = [value]
    elif field == "alignment":
        predicate["alignment"] = [value]
    elif field == "atr_band":
        if value == "lt_0.3pct":
            predicate["atr_pct"] = {"lt": 0.003}
        elif value == "0.3_to_0.6pct":
            predicate["atr_pct"] = {"between": [0.003, 0.006]}
        elif value == "0.6_to_1.0pct":
            predicate["atr_pct"] = {"between": [0.006, 0.010]}
        elif value == "gte_1.0pct":
            predicate["atr_pct"] = {"gt": 0.010}
    elif field == "slippage_band":
        if value == "lt_2bps":
            predicate["slippage_bps"] = {"lt": 2.0}
        elif value == "2_to_5bps":
            predicate["slippage_bps"] = {"between": [2.0, 5.0]}
        elif value == "5_to_10bps":
            predicate["slippage_bps"] = {"between": [5.0, 10.0]}
        elif value == "gte_10bps":
            predicate["slippage_bps"] = {"gt": 10.0}
    elif field == "score_margin":
        predicate["score_margin"] = [value]

    if negative:
        effect["risk_scalar"] = 0.60 if stats.avg_r < 0 else 0.80
        effect["buy_th_add"] = 0.15
        effect["sell_th_add"] = 0.15
        if field == "side" and value == "BUY" and stats.avg_r < -0.20 and stats.win_rate < 0.35:
            effect["block_buy"] = True
            effect["risk_scalar"] = 0.40
        if field == "side" and value == "SELL" and stats.avg_r < -0.20 and stats.win_rate < 0.35:
            effect["block_sell"] = True
            effect["risk_scalar"] = 0.40
        if field == "alignment" and value == "counter":
            effect["risk_scalar"] = min(float(effect["risk_scalar"]), 0.55)
    else:
        effect["risk_scalar"] = 1.15 if stats.avg_r > 0 else 1.05
        if field == "alignment" and value == "aligned":
            effect["risk_scalar"] = max(float(effect["risk_scalar"]), 1.20)

    name = f"{'de' if negative else 'up'}risk_{field}_{value}"
    note = f"{field}={value},avg_r={stats.avg_r:.3f},win={stats.win_rate:.2f}"
    return Rule(name=name, predicate=predicate, effect=effect, note=note)


def generate_rules(stats: dict[str, Any], constraints: DiagnosticsConstraints | dict[str, Any] | None = None) -> PolicyRecipe:
    if isinstance(constraints, DiagnosticsConstraints):
        cfg = constraints
        extra: dict[str, Any] = {}
    else:
        constraints = constraints or {}
        cfg = DiagnosticsConstraints(
            min_samples_per_rule=int(constraints.get("min_samples_per_rule", 30)),
            objective_lambda=float(constraints.get("objective_lambda", 100.0)),
            top_k=int(constraints.get("top_k", 8)),
            min_objective_delta_per_trade=float(constraints.get("min_objective_delta_per_trade", 0.0)),
        )
        extra = dict(constraints)

    validation_stats: dict[str, SegmentStats] = extra.get("validation_stats") or {}
    baseline_validation_stats: SegmentStats | None = extra.get("baseline_validation_stats")
    if baseline_validation_stats is None:
        all_rows: list[FeatureRow] = list(extra.get("validation_rows") or [])
        baseline_validation_stats = _calc_stats(all_rows, cfg.objective_lambda)
    baseline_rate = (
        float(baseline_validation_stats.objective) / float(max(1, baseline_validation_stats.count))
        if baseline_validation_stats
        else 0.0
    )

    candidates: list[tuple[float, Rule]] = []
    for key in sorted(stats.keys()):
        train_stats = stats.get(key)
        if not isinstance(train_stats, SegmentStats):
            continue
        val_stats = validation_stats.get(key)
        if not isinstance(val_stats, SegmentStats):
            continue
        if train_stats.count < int(cfg.min_samples_per_rule) or val_stats.count <= 0:
            continue
        seg_rate = float(val_stats.objective) / float(max(1, val_stats.count))
        delta = seg_rate - baseline_rate
        if abs(delta) <= float(cfg.min_objective_delta_per_trade):
            continue
        negative = bool(delta < 0)
        rule = _rule_for_bucket(key, negative=negative, stats=val_stats)
        score = abs(delta) * math.sqrt(float(val_stats.count))
        candidates.append((float(score), rule))

    candidates.sort(key=lambda x: (-x[0], x[1].name))
    selected = [rule for _, rule in candidates[: max(0, int(cfg.top_k))]]
    source_range = extra.get("source_range")
    if source_range is None:
        ts_vals = [int(r.ts_ms) for r in list(extra.get("rows_all") or []) if int(r.ts_ms) > 0]
        if ts_vals:
            source_range = {"start_ms": int(min(ts_vals)), "end_ms": int(max(ts_vals))}
    generated_at = "1970-01-01T00:00:00+00:00"
    if isinstance(source_range, dict) and source_range.get("end_ms"):
        try:
            generated_at = datetime.fromtimestamp(float(source_range["end_ms"]) / 1000.0, tz=UTC).isoformat()
        except Exception:
            generated_at = "1970-01-01T00:00:00+00:00"
    meta = {
        "generated_at": generated_at,
        "objective_lambda": float(cfg.objective_lambda),
        "min_samples_per_rule": int(cfg.min_samples_per_rule),
        "top_k": int(cfg.top_k),
        "candidate_count": int(len(candidates)),
        "selected_count": int(len(selected)),
        "baseline_validation_objective": float(baseline_validation_stats.objective if baseline_validation_stats else 0.0),
        "source_range": source_range,
    }
    return PolicyRecipe(rules=selected, meta=meta)


def rule_to_dict(rule: Rule) -> dict[str, Any]:
    return {
        "name": rule.name,
        "when": dict(rule.predicate or {}),
        "then": dict(rule.effect or {}),
        "note": rule.note,
    }


def policy_recipe_to_dict(recipe: PolicyRecipe) -> dict[str, Any]:
    return {
        "version": 1,
        "rules": [rule_to_dict(r) for r in recipe.rules],
        "meta": dict(recipe.meta or {}),
    }


def policy_recipe_from_dict(payload: dict[str, Any]) -> PolicyRecipe:
    rules_raw = payload.get("rules") if isinstance(payload, dict) else []
    rules: list[Rule] = []
    for i, rr in enumerate(rules_raw or []):
        if not isinstance(rr, dict):
            continue
        name = str(rr.get("name") or rr.get("id") or f"rule_{i:03d}")
        pred = rr.get("when")
        if pred is None:
            pred = rr.get("predicate")
        eff = rr.get("then")
        if eff is None:
            eff = rr.get("effect")
        rules.append(
            Rule(
                name=name,
                predicate=dict(pred or {}),
                effect=dict(eff or {}),
                note=str(rr.get("note")) if rr.get("note") is not None else None,
            )
        )
    meta = payload.get("meta") if isinstance(payload, dict) else {}
    return PolicyRecipe(rules=rules, meta=dict(meta or {}))


def segment_stats_to_dict(stats: SegmentStats) -> dict[str, Any]:
    return asdict(stats)
