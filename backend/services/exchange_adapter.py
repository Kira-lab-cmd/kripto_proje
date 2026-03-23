# File: backend/services/exchange_adapter.py
from __future__ import annotations

import asyncio
from typing import List

class BinanceServiceAdapter:
    """
    Adapter for your existing binance_svc.get_historical_data(symbol, timeframe, limit).
    Also provides now_ms with exchange clock source if available; otherwise local.
    """
    def __init__(self, binance_svc, logger):
        self._svc = binance_svc
        self._log = logger

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int) -> list[list[float]]:
        # If your binance_svc is sync, run in executor to keep event loop safe.
        fn = getattr(self._svc, "get_historical_data", None)
        if fn is None:
            raise RuntimeError("binance_svc.get_historical_data not found")

        if asyncio.iscoroutinefunction(fn):
            data = await fn(symbol, timeframe, limit=limit)
        else:
            loop = asyncio.get_running_loop()
            data = await loop.run_in_executor(None, lambda: fn(symbol, timeframe, limit=limit))
        return data

    async def now_ms(self) -> int:
        # Prefer exchange time if your service exposes it; fallback to local.
        tfn = getattr(self._svc, "get_exchange_time_ms", None)
        if tfn:
            if asyncio.iscoroutinefunction(tfn):
                return int(await tfn())
            loop = asyncio.get_running_loop()
            return int(await loop.run_in_executor(None, lambda: tfn()))
        import time
        return int(time.time() * 1000)