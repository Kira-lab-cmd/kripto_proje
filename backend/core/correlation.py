# File: backend/core/correlation.py
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Protocol, Tuple


class ResearchStoreLike(Protocol):
    def load(self, symbol: str, timeframe: str, since_ms: int) -> list[tuple[int, float, float, float, float, float]]: ...


@dataclass(frozen=True)
class CorrelationConfig:
    timeframe: str = "1h"
    lookback_days: int = 60
    ttl_seconds: int = 6 * 60 * 60  # 6h
    min_bars: int = 200
    corr_warn: float = 0.75
    corr_block: float = 0.90


@dataclass(frozen=True)
class CorrelationState:
    matrix: Dict[Tuple[str, str], float]
    bars: Dict[str, int]
    since_ms: int
    computed_at_s: float


def _pearson_corr(x: list[float], y: list[float]) -> float:
    n = min(len(x), len(y))
    if n < 3:
        return 0.0
    x = x[-n:]
    y = y[-n:]
    mx = sum(x) / n
    my = sum(y) / n
    num = 0.0
    dx2 = 0.0
    dy2 = 0.0
    for i in range(n):
        dx = x[i] - mx
        dy = y[i] - my
        num += dx * dy
        dx2 += dx * dx
        dy2 += dy * dy
    den = (dx2 * dy2) ** 0.5
    if den <= 0:
        return 0.0
    return float(num / den)


def _returns_from_closes(closes: list[float]) -> list[float]:
    rets: list[float] = []
    for i in range(1, len(closes)):
        p0 = float(closes[i - 1])
        p1 = float(closes[i])
        if p0 > 0:
            rets.append((p1 / p0) - 1.0)
    return rets


class CorrelationService:
    """Computes and caches rolling correlation matrix from research.db.

    Purpose: correlation-aware exposure control.
    The service is intentionally offline-data-based (research.db) because
    correlation is a slow-moving risk metric; it does not need per-loop ccxt.
    """

    def __init__(self, store: ResearchStoreLike, cfg: CorrelationConfig, logger):
        self._store = store
        self._cfg = cfg
        self._log = logger
        self._cache: Optional[CorrelationState] = None
        self._lock = asyncio.Lock()

    def _is_fresh(self, st: CorrelationState) -> bool:
        return (time.time() - st.computed_at_s) < self._cfg.ttl_seconds

    async def get(self, symbols: Iterable[str]) -> CorrelationState:
        st = self._cache
        if st and self._is_fresh(st):
            return st

        async with self._lock:
            st2 = self._cache
            if st2 and self._is_fresh(st2):
                return st2
            new_state = await asyncio.to_thread(self._compute_sync, list(symbols))
            self._cache = new_state
            return new_state

    def _compute_sync(self, symbols: list[str]) -> CorrelationState:
        now_ms = int(time.time() * 1000)
        since_ms = now_ms - int(self._cfg.lookback_days * 24 * 60 * 60 * 1000)

        # Load closes per symbol
        closes_by_sym: Dict[str, list[float]] = {}
        bars: Dict[str, int] = {}
        for s in symbols:
            try:
                rows = self._store.load(s, self._cfg.timeframe, since_ms)
                closes = [float(r[4]) for r in rows if r and len(r) >= 5]
                bars[s] = len(closes)
                if len(closes) >= self._cfg.min_bars:
                    closes_by_sym[s] = closes
            except Exception as e:
                self._log.debug("corr_load_failed", extra={"symbol": s, "err": str(e)})

        rets_by_sym: Dict[str, list[float]] = {s: _returns_from_closes(c) for s, c in closes_by_sym.items()}

        matrix: Dict[Tuple[str, str], float] = {}
        keys = sorted(rets_by_sym.keys())
        for i, a in enumerate(keys):
            for b in keys[i:]:
                if a == b:
                    matrix[(a, b)] = 1.0
                    continue
                ca = rets_by_sym[a]
                cb = rets_by_sym[b]
                corr = _pearson_corr(ca, cb)
                matrix[(a, b)] = corr
                matrix[(b, a)] = corr

        self._log.debug(
            "corr_matrix_computed",
            extra={"tf": self._cfg.timeframe, "lookback_days": self._cfg.lookback_days, "symbols": keys, "bars": bars},
        )

        return CorrelationState(matrix=matrix, bars=bars, since_ms=since_ms, computed_at_s=time.time())


def correlation_penalty(
    *,
    st: CorrelationState,
    candidate_symbol: str,
    open_symbols: Iterable[str],
    warn: float,
    block: float,
) -> tuple[float, str | None]:
    """Returns (risk_multiplier_factor, reason).

    - If max corr >= block: factor=0.0 (block)
    - If max corr in [warn, block): factor in {0.7, 0.5} tiered
    - else: factor=1.0
    """

    max_corr = 0.0
    max_sym = None
    for s in open_symbols:
        c = float(st.matrix.get((candidate_symbol, s), 0.0))
        if c > max_corr:
            max_corr = c
            max_sym = s

    if max_sym is None:
        return 1.0, None

    if max_corr >= block:
        return 0.0, f"corr_block({max_sym} corr={max_corr:.2f})"
    if max_corr >= warn:
        if max_corr >= (warn + (block - warn) * 0.5):
            return 0.5, f"corr_reduce50({max_sym} corr={max_corr:.2f})"
        return 0.7, f"corr_reduce30({max_sym} corr={max_corr:.2f})"
    return 1.0, None