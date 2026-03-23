# File: backend/app_wiring/trend_bias_wiring.py
from __future__ import annotations

from ..core.trend_bias import TrendBiasConfig, TrendBiasService
from ..services.exchange_adapter import BinanceServiceAdapter


def build_trend_bias_service(*, binance_svc, logger) -> TrendBiasService:
    ex = BinanceServiceAdapter(binance_svc, logger)
    cfg = TrendBiasConfig(
        timeframe="1h",
        limit=250,
        ttl_seconds=55 * 60,
        slope_lookback=6,
        slope_min=0.0002,
        ema_fast=50,
        ema_slow=200,
    )
    return TrendBiasService(ex, cfg, logger)