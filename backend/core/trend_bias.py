# File: backend/core/trend_bias.py
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Optional, Protocol, Tuple

from .indicators_light import ema, slope_normalized
from .ohlcv_utils import drop_unclosed_last_candle, to_close_series
from .timeframes import timeframe_seconds


class ExchangeLike(Protocol):
    """
    Minimal interface your binance_service can implement/adapt to.
    """
    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int) -> list[list[float]]: ...
    async def now_ms(self) -> int: ...


@dataclass(frozen=True)
class TrendBiasConfig:
    timeframe: str = "1h"
    limit: int = 250
    ttl_seconds: int = 55 * 60  # update roughly once per hour
    slope_lookback: int = 6      # 6 hours
    slope_min: float = 0.0002    # ~2 bps over lookback on EMA50
    ema_fast: int = 50
    ema_slow: int = 200


@dataclass(frozen=True)
class TrendBiasState:
    direction: str  # "UP" | "DOWN" | "NEUTRAL" | "UNKNOWN"
    close: float
    ema50: float
    ema200: float
    slope: float
    ts_ms: int      # timestamp of last used candle
    computed_at_s: float


class TrendBiasService:
    """
    Caches per-symbol 1h trend direction with TTL.
    Safe for concurrent async access (per-symbol locks).
    """
    def __init__(self, exchange: ExchangeLike, cfg: TrendBiasConfig, logger):
        self._ex = exchange
        self._cfg = cfg
        self._log = logger
        self._cache: Dict[str, TrendBiasState] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def _lock_for(self, symbol: str) -> asyncio.Lock:
        if symbol not in self._locks:
            self._locks[symbol] = asyncio.Lock()
        return self._locks[symbol]

    def _is_fresh(self, st: TrendBiasState) -> bool:
        return (time.time() - st.computed_at_s) < self._cfg.ttl_seconds

    async def get(self, symbol: str) -> TrendBiasState:
        st = self._cache.get(symbol)
        if st and self._is_fresh(st):
            return st

        lock = self._lock_for(symbol)
        async with lock:
            st2 = self._cache.get(symbol)
            if st2 and self._is_fresh(st2):
                return st2
            new_state = await self._compute(symbol)
            self._cache[symbol] = new_state
            return new_state

    async def _compute(self, symbol: str) -> TrendBiasState:
        tf = self._cfg.timeframe
        try:
            now_ms = await self._ex.now_ms()
            raw = await self._ex.fetch_ohlcv(symbol, tf, self._cfg.limit)
            raw = drop_unclosed_last_candle(raw, tf, now_ms)
            if len(raw) < max(self._cfg.ema_slow, self._cfg.ema_fast) + self._cfg.slope_lookback + 5:
                self._log.warning("trend_bias_insufficient_bars", extra={"symbol": symbol, "bars": len(raw), "tf": tf})
                return TrendBiasState(
                    direction="UNKNOWN", close=0.0, ema50=0.0, ema200=0.0, slope=0.0,
                    ts_ms=raw[-1][0] if raw else 0, computed_at_s=time.time()
                )

            closes = to_close_series(raw)

            ema50_series = ema(closes, self._cfg.ema_fast)
            ema200_series = ema(closes, self._cfg.ema_slow)
            if not ema50_series or not ema200_series:
                return TrendBiasState(
                    direction="UNKNOWN", close=closes[-1], ema50=0.0, ema200=0.0, slope=0.0,
                    ts_ms=int(raw[-1][0]), computed_at_s=time.time()
                )

            ema50_last = float(ema50_series[-1])
            ema200_last = float(ema200_series[-1])
            close_last = float(closes[-1])

            slope = float(slope_normalized(ema50_series, self._cfg.slope_lookback))

            direction = "NEUTRAL"
            if close_last > ema200_last and ema50_last > ema200_last and slope > self._cfg.slope_min:
                direction = "UP"
            elif close_last < ema200_last and ema50_last < ema200_last and slope < -self._cfg.slope_min:
                direction = "DOWN"

            st = TrendBiasState(
                direction=direction,
                close=close_last,
                ema50=ema50_last,
                ema200=ema200_last,
                slope=slope,
                ts_ms=int(raw[-1][0]),
                computed_at_s=time.time(),
            )

            self._log.debug(
                "trend_bias_computed",
                extra={
                    "symbol": symbol,
                    "tf": tf,
                    "direction": direction,
                    "close": close_last,
                    "ema50": ema50_last,
                    "ema200": ema200_last,
                    "slope": slope,
                    "last_ts_ms": st.ts_ms,
                },
            )
            return st

        except Exception as e:
            self._log.exception("trend_bias_compute_failed", extra={"symbol": symbol, "tf": tf, "err": str(e)})
            return TrendBiasState(
                direction="UNKNOWN", close=0.0, ema50=0.0, ema200=0.0, slope=0.0, ts_ms=0, computed_at_s=time.time()
            )