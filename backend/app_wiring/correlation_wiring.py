# File: backend/app_wiring/correlation_wiring.py
from __future__ import annotations

import os

from backend.core.correlation import CorrelationConfig, CorrelationService
from backend.research_store import ResearchStore


def build_correlation_service(*, research_store: ResearchStore, logger) -> CorrelationService:
    cfg = CorrelationConfig(
        timeframe=os.getenv("CORR_TF", "1h").strip() or "1h",
        lookback_days=int(os.getenv("CORR_LOOKBACK_DAYS", "60")),
        ttl_seconds=int(os.getenv("CORR_TTL_S", str(6 * 60 * 60))),
        min_bars=int(os.getenv("CORR_MIN_BARS", "200")),
        corr_warn=float(os.getenv("CORR_WARN", "0.75")),
        corr_block=float(os.getenv("CORR_BLOCK", "0.90")),
    )
    return CorrelationService(research_store, cfg, logger)
