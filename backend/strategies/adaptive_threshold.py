# File: backend/strategies/adaptive_threshold.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

TrendDir = Literal["UP", "DOWN", "NEUTRAL", "UNKNOWN"]


@dataclass(frozen=True)
class AdaptiveThresholdConfig:
    base_threshold: int = 60
    aligned_delta: int = -5    # aligned => threshold drops
    neutral_delta: int = 0
    opposed_delta: int = +10   # opposed => threshold increases


def adjust_threshold_for_1h(
    *,
    base_threshold: int,
    trade_side: Literal["LONG", "SHORT"],
    dir_1h: TrendDir,
    cfg: AdaptiveThresholdConfig,
) -> int:
    """
    Only used in 15m TREND regime.
    CHOP ignores.
    Deterministic mapping.
    """
    if dir_1h not in ("UP", "DOWN", "NEUTRAL"):
        return base_threshold  # UNKNOWN => no bias

    aligned = (trade_side == "LONG" and dir_1h == "UP") or (trade_side == "SHORT" and dir_1h == "DOWN")
    opposed = (trade_side == "LONG" and dir_1h == "DOWN") or (trade_side == "SHORT" and dir_1h == "UP")

    if aligned:
        return max(0, base_threshold + cfg.aligned_delta)
    if opposed:
        return min(100, base_threshold + cfg.opposed_delta)
    return base_threshold + cfg.neutral_delta