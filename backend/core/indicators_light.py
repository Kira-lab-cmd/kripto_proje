# File: backend/core/indicators_light.py
from __future__ import annotations

from typing import List


def ema(values: List[float], period: int) -> List[float]:
    if period <= 1:
        raise ValueError("period must be > 1")
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    out: List[float] = []
    # start with SMA seed
    seed = sum(values[:period]) / period
    out.append(seed)
    for v in values[period:]:
        out.append((v - out[-1]) * k + out[-1])
    # pad to align length with input (optional): we return aligned tail only
    return out


def slope_normalized(values: List[float], lookback: int) -> float:
    """
    Simple normalized slope: (last - prev) / prev over lookback.
    Deterministic, cheap, no regression.
    """
    if len(values) < lookback + 1:
        return 0.0
    a = values[-(lookback + 1)]
    b = values[-1]
    if a == 0:
        return 0.0
    return (b - a) / a