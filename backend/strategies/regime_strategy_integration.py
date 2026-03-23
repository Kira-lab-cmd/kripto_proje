# File: backend/strategies/regime_strategy_integration.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional

from backend.core.trend_bias import TrendBiasService
from backend.strategies.adaptive_threshold import AdaptiveThresholdConfig, adjust_threshold_for_1h


@dataclass(frozen=True)
class StrategyDecision:
    action: Literal["ENTER", "HOLD"]
    side: Optional[Literal["LONG", "SHORT"]]
    score: float
    threshold: int
    reason: str
    meta: Dict[str, Any]


class RegimeAwareStrategy:
    """
    Wrap your existing scoring engine / regime detector.
    This class only shows how to apply 1h adaptive bias in TREND regime.
    """
    def __init__(self, *, trend_bias_svc: TrendBiasService, logger, thr_cfg: AdaptiveThresholdConfig):
        self._trend_bias = trend_bias_svc
        self._log = logger
        self._thr_cfg = thr_cfg

    async def decide(
        self,
        *,
        symbol: str,
        regime_15m: str,  # "TREND"|"CHOP"|"HIGH_VOL"
        trade_side: Literal["LONG", "SHORT"],
        score_15m: float,
        base_threshold: int,
        audit_ctx: Dict[str, Any],
    ) -> StrategyDecision:

        threshold = base_threshold
        dir_1h = "UNKNOWN"

        if regime_15m == "TREND":
            st = await self._trend_bias.get(symbol)
            dir_1h = st.direction
            threshold = adjust_threshold_for_1h(
                base_threshold=base_threshold,
                trade_side=trade_side,
                dir_1h=dir_1h,
                cfg=self._thr_cfg,
            )

        # CHOP: ignore 1h
        # HIGH_VOL: handled elsewhere (hard gate)

        action = "ENTER" if score_15m >= threshold else "HOLD"
        reason = "score_below_threshold"
        if action == "ENTER":
            reason = "score_meets_threshold"

        meta = {
            **audit_ctx,
            "regime_15m": regime_15m,
            "score_15m": score_15m,
            "threshold": threshold,
            "base_threshold": base_threshold,
            "dir_1h": dir_1h,
        }

        self._log.info("strategy_decision", extra=meta)

        return StrategyDecision(
            action=action,
            side=trade_side if action == "ENTER" else None,
            score=score_15m,
            threshold=threshold,
            reason=reason,
            meta=meta,
        )