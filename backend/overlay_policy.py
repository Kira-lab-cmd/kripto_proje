from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .edge_diagnostics import PolicyRecipe


@dataclass
class OverlayContext:
    ts_ms: int | None = None
    sym: str | None = None
    side: str | None = None
    current_price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    qty_prelim: float | None = None
    risk_pct: float | None = None
    base_risk_pct: float | None = None
    regime: str | None = None
    adx: float | None = None
    er: float | None = None
    atr_pct: float | None = None
    trend_dir_1h: str | None = None
    score: float | None = None
    buy_th: float | None = None
    sell_th: float | None = None
    reason: str | None = None
    # Backward-compatible alias used by existing hooks.
    symbol: str | None = None

    def __post_init__(self) -> None:
        if self.sym is None and self.symbol is not None:
            self.sym = str(self.symbol)
        if self.symbol is None and self.sym is not None:
            self.symbol = str(self.sym)


@dataclass
class OverlayDecision:
    block_buy: bool = False
    block_sell: bool = False
    buy_th_add: float = 0.0
    sell_th_add: float = 0.0
    risk_scalar: float = 1.0
    note: str | None = None


class OverlayPolicy(Protocol):
    def decide(self, ctx: OverlayContext) -> OverlayDecision:
        ...


class DefaultOverlayPolicy(OverlayPolicy):
    def decide(self, ctx: OverlayContext) -> OverlayDecision:
        del ctx
        return OverlayDecision()


class AtrRegimeOverlayPolicy(OverlayPolicy):
    """
    Example: target ATR and regime-based scaling.
    This is where you put the "real overlay" logic.
    """

    def __init__(
        self,
        target_atr: float = 0.004,
        atr_min: float = 0.0025,
        atr_max: float = 0.010,
        trend_scalar: float = 1.10,
        soft_trend_scalar: float = 0.90,
        chop_scalar: float = 0.65,
        clamp_lo: float = 0.50,
        clamp_hi: float = 1.50,
    ):
        self.target_atr = target_atr
        self.atr_min = atr_min
        self.atr_max = atr_max
        self.trend_scalar = trend_scalar
        self.soft_trend_scalar = soft_trend_scalar
        self.chop_scalar = chop_scalar
        self.clamp_lo = clamp_lo
        self.clamp_hi = clamp_hi

    def decide(self, ctx: OverlayContext) -> OverlayDecision:
        s = 1.0
        buy_th_add = 0.0
        sell_th_add = 0.0
        block_buy = False
        block_sell = False
        notes: list[str] = []

        # ATR scaling
        if ctx.atr_pct and ctx.atr_pct > 0:
            atr_s = self.target_atr / ctx.atr_pct
            atr_s = max(self.clamp_lo, min(self.clamp_hi, atr_s))
            if ctx.atr_pct < self.atr_min:
                atr_s *= 0.75
                notes.append("atr<min de-risk")
                buy_th_add += 0.20
                sell_th_add += 0.20
            if ctx.atr_pct > self.atr_max:
                atr_s *= 0.75
                notes.append("atr>max de-risk")
                buy_th_add += 0.20
                sell_th_add += 0.20
            s *= atr_s
            notes.append(f"atr_s={atr_s:.2f}")
        else:
            buy_th_add += 0.10
            sell_th_add += 0.10
            notes.append("atr=unknown tighten")

        # Regime scaling
        reg = (ctx.regime or "UNKNOWN").upper()
        if reg == "TREND":
            s *= self.trend_scalar
            notes.append(f"reg=TREND x{self.trend_scalar:.2f}")
        elif reg == "SOFT_TREND":
            s *= self.soft_trend_scalar
            notes.append(f"reg=SOFT x{self.soft_trend_scalar:.2f}")
        elif reg == "CHOP":
            s *= self.chop_scalar
            buy_th_add += 0.25
            sell_th_add += 0.25
            notes.append(f"reg=CHOP x{self.chop_scalar:.2f}")
        elif reg == "CRASH":
            block_buy = True
            s *= 0.5
            notes.append("reg=CRASH block_buy")

        trend = (ctx.trend_dir_1h or "").upper()
        side = (ctx.side or "").upper()
        if trend == "UP" and side == "SELL":
            block_sell = True
            notes.append("1h_up block_sell")
        elif trend == "DOWN" and side == "BUY":
            block_buy = True
            notes.append("1h_down block_buy")
        elif trend == "NEUTRAL":
            buy_th_add += 0.10
            sell_th_add += 0.10
            notes.append("1h_neutral tighten")

        s = max(0.0, min(2.0, s))

        return OverlayDecision(
            block_buy=bool(block_buy),
            block_sell=bool(block_sell),
            buy_th_add=float(max(0.0, buy_th_add)),
            sell_th_add=float(max(0.0, sell_th_add)),
            risk_scalar=float(s),
            note="; ".join(notes) if notes else None,
        )


class DataDrivenOverlayPolicy(OverlayPolicy):
    def __init__(
        self,
        recipe: "PolicyRecipe | dict[str, Any] | str | None" = None,
        policy_path: str | None = None,
        *,
        rules: list[Any] | None = None,
    ):
        if isinstance(recipe, str) and policy_path is None and rules is None:
            policy_path = recipe
            recipe = None
        self._rules: list[dict[str, Any]] = []
        if recipe is not None:
            self._load_from_recipe(recipe)
        elif rules is not None:
            self._rules = [self._normalize_rule(r, idx=i) for i, r in enumerate(rules)]
        elif policy_path:
            self._load_from_path(policy_path)

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    def _normalize_rule(self, rule: Any, *, idx: int) -> dict[str, Any]:
        if hasattr(rule, "__dict__") and not isinstance(rule, dict):
            raw = dict(getattr(rule, "__dict__", {}))
        elif isinstance(rule, dict):
            raw = dict(rule)
        else:
            raw = {}
        name = str(raw.get("name") or raw.get("id") or f"rule_{idx:03d}")
        when = raw.get("when")
        if when is None:
            when = raw.get("predicate")
        then = raw.get("then")
        if then is None:
            then = raw.get("effect")
        return {
            "name": name,
            "when": dict(when or {}),
            "then": dict(then or {}),
            "note": (str(raw.get("note")) if raw.get("note") is not None else None),
        }

    def _load_from_recipe(self, recipe: "PolicyRecipe | dict[str, Any]") -> None:
        raw = dict(getattr(recipe, "__dict__", {})) if hasattr(recipe, "__dict__") and not isinstance(recipe, dict) else dict(recipe or {})
        rules = raw.get("rules")
        if not isinstance(rules, list):
            self._rules = []
            return
        self._rules = [self._normalize_rule(r, idx=i) for i, r in enumerate(rules)]

    def _load_from_path(self, policy_path: str) -> None:
        path = Path(str(policy_path))
        if not path.exists():
            logger.warning("DataDrivenOverlayPolicy: policy file not found: %s", path)
            self._rules = []
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("DataDrivenOverlayPolicy: failed to read policy file %s (%s)", path, exc)
            self._rules = []
            return
        if not isinstance(payload, dict):
            logger.warning("DataDrivenOverlayPolicy: invalid policy root type: %s", type(payload).__name__)
            self._rules = []
            return
        self._load_from_recipe(payload)

    @staticmethod
    def _field_value(ctx: OverlayContext, field: str) -> Any:
        if field == "symbol":
            return ctx.symbol if ctx.symbol is not None else ctx.sym
        if field == "sym":
            return ctx.sym if ctx.sym is not None else ctx.symbol
        return getattr(ctx, field, None)

    @staticmethod
    def _eval_op(lhs: Any, *, op: str, rhs: Any) -> bool:
        if op == "in":
            if rhs is None:
                return False
            seq = rhs if isinstance(rhs, (list, tuple, set)) else [rhs]
            return lhs in seq
        if op == "not_in":
            if rhs is None:
                return True
            seq = rhs if isinstance(rhs, (list, tuple, set)) else [rhs]
            return lhs not in seq

        try:
            a = float(lhs)
            b = float(rhs)
            if op == ">":
                return a > b
            if op == ">=":
                return a >= b
            if op == "<":
                return a < b
            if op == "<=":
                return a <= b
        except Exception:
            pass

        if op == "==":
            return lhs == rhs
        if op == "!=":
            return lhs != rhs
        return False

    def _eval_condition(self, lhs: Any, cond: Any) -> bool:
        if isinstance(cond, dict) and "op" in cond:
            return self._eval_op(lhs, op=str(cond.get("op")), rhs=cond.get("value"))

        # Backward-compatible condition formats generated by existing policy_generator.
        if isinstance(cond, dict):
            if "lt" in cond and not self._eval_op(lhs, op="<", rhs=cond.get("lt")):
                return False
            if "gt" in cond and not self._eval_op(lhs, op=">", rhs=cond.get("gt")):
                return False
            if "between" in cond:
                rng = cond.get("between")
                if not (isinstance(rng, (list, tuple)) and len(rng) == 2):
                    return False
                if not (self._eval_op(lhs, op=">=", rhs=rng[0]) and self._eval_op(lhs, op="<=", rhs=rng[1])):
                    return False
            if "op" not in cond and not any(k in cond for k in ("lt", "gt", "between")):
                return lhs == cond
            return True

        if isinstance(cond, (list, tuple, set)):
            return lhs in cond
        return lhs == cond

    def _match_rule(self, rule: dict[str, Any], ctx: OverlayContext) -> bool:
        when = rule.get("when")
        if not isinstance(when, dict):
            return True
        for field, cond in when.items():
            lhs = self._field_value(ctx, str(field))
            if not self._eval_condition(lhs, cond):
                return False
        return True

    def decide(self, ctx: OverlayContext) -> OverlayDecision:
        if not self._rules:
            return OverlayDecision()

        block_buy = False
        block_sell = False
        buy_add = 0.0
        sell_add = 0.0
        risk_scalar = 1.0
        notes: list[str] = []

        for rule in self._rules:
            if not self._match_rule(rule, ctx):
                continue
            then = rule.get("then")
            if not isinstance(then, dict):
                continue
            block_buy = bool(block_buy or bool(then.get("block_buy", False)))
            block_sell = bool(block_sell or bool(then.get("block_sell", False)))
            buy_add += self._safe_float(then.get("buy_th_add"), 0.0)
            sell_add += self._safe_float(then.get("sell_th_add"), 0.0)
            risk_scalar *= self._safe_float(then.get("risk_scalar"), 1.0)
            note = then.get("note")
            if note is None:
                note = rule.get("note")
            if note is None:
                note = rule.get("name")
            if note is not None and str(note).strip():
                notes.append(str(note).strip())

        if not notes and not (block_buy or block_sell or buy_add or sell_add or risk_scalar != 1.0):
            return OverlayDecision()
        return OverlayDecision(
            block_buy=bool(block_buy),
            block_sell=bool(block_sell),
            buy_th_add=float(buy_add),
            sell_th_add=float(sell_add),
            risk_scalar=float(risk_scalar),
            note="; ".join(notes) if notes else None,
        )


def load_policy_recipe(path: str) -> "PolicyRecipe":
    from .edge_diagnostics import PolicyRecipe, policy_recipe_from_dict

    p = Path(str(path))
    if not p.exists():
        logger.warning("load_policy_recipe: file not found: %s", p)
        return PolicyRecipe(rules=[], meta={"error": "file_not_found", "path": str(p)})
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("load_policy_recipe: failed to read %s (%s)", p, exc)
        return PolicyRecipe(rules=[], meta={"error": "invalid_json", "path": str(p)})
    if not isinstance(payload, dict):
        logger.warning("load_policy_recipe: invalid root type in %s", p)
        return PolicyRecipe(rules=[], meta={"error": "invalid_root", "path": str(p)})
    return policy_recipe_from_dict(payload)


def overlay_policy_from_recipe(recipe: dict[str, Any] | None) -> OverlayPolicy:
    if not isinstance(recipe, dict) or not isinstance(recipe.get("rules"), list):
        return DefaultOverlayPolicy()
    return DataDrivenOverlayPolicy(recipe=recipe)


def build_overlay_policy(name: str, recipe_path: str | None = None, **kwargs: Any) -> OverlayPolicy:
    mode = str(name or "none").strip().lower()
    if mode == "none":
        return DefaultOverlayPolicy()
    if mode == "atr_regime":
        return AtrRegimeOverlayPolicy(
            target_atr=float(kwargs.get("target_atr", 0.004)),
            atr_min=float(kwargs.get("atr_min", 0.0025)),
            atr_max=float(kwargs.get("atr_max", 0.010)),
            trend_scalar=float(kwargs.get("trend_scalar", 1.10)),
            soft_trend_scalar=float(kwargs.get("soft_trend_scalar", 0.90)),
            chop_scalar=float(kwargs.get("chop_scalar", 0.65)),
            clamp_lo=float(kwargs.get("clamp_lo", 0.50)),
            clamp_hi=float(kwargs.get("clamp_hi", 1.50)),
        )
    if mode in {"data", "data_driven"}:
        recipe = kwargs.get("recipe")
        rules = kwargs.get("rules")
        if recipe is not None or rules is not None:
            return DataDrivenOverlayPolicy(recipe=recipe, rules=rules)
        if recipe_path:
            return overlay_policy_from_recipe(kwargs.get("recipe") or dict(load_policy_recipe(recipe_path).__dict__))
        return DefaultOverlayPolicy()
    return DefaultOverlayPolicy()
