# File: backend/core/ohlcv_utils.py
from __future__ import annotations

from typing import Iterable, List, Sequence, Tuple

from .timeframes import timeframe_seconds


def drop_unclosed_last_candle(ohlcv: Sequence[Sequence[float]], timeframe: str, now_ms: int) -> List[List[float]]:
    """
    Drops the last candle if it is not fully closed yet.
    Works for exchange timestamps in ms.
    """
    if not ohlcv:
        return []
    tf_ms = timeframe_seconds(timeframe) * 1000
    last_ts = int(ohlcv[-1][0])
    # Candle is considered closed if now >= last_ts + tf_ms
    if now_ms < last_ts + tf_ms:
        return [list(x) for x in ohlcv[:-1]]
    return [list(x) for x in ohlcv]


def to_close_series(ohlcv: Sequence[Sequence[float]]) -> List[float]:
    return [float(x[4]) for x in ohlcv]


def to_volume_series(ohlcv: Sequence[Sequence[float]]) -> List[float]:
    return [float(x[5]) for x in ohlcv]