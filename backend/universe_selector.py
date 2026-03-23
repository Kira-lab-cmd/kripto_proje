from __future__ import annotations

"""Universe selection (Binance-only).

Goal: pick a small set of symbols that are:
  - liquid (24h quote volume)
  - tight spread
  - trend-friendly (ADX/ER + low chop)

This module is intentionally deterministic and conservative.
"""

import math
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from .regime import RegimeDetector
from .utils_symbols import normalize_symbol


logger = logging.getLogger(__name__)
UTC_TZ = ZoneInfo("UTC")


STABLE_BASES = {
    "USDT",
    "USDC",
    "BUSD",
    "TUSD",
    "FDUSD",
    "DAI",
    "PAX",
    "USDP",
}


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(x)))


def to_binance_symbol(ccxt_symbol: str) -> str:
    return str(ccxt_symbol or "").replace("/", "")


@dataclass(frozen=True)
class UniverseConfig:
    anchors: tuple[str, ...] = ("BTC/USDT", "ETH/USDT")
    dynamic_n: int = 6
    min_quote_volume_usd: float = 50_000_000.0
    max_spread_pct: float = 0.25  # percent
    timeframe: str = "1h"
    trend_lookback_bars: int = 600  # ~25 days on 1h
    top_by_volume_for_trend_eval: int = 30
    excluded_symbols: tuple[str, ...] = ("PAXG/USDT", "DOGE/USDT")
    excluded_bases: tuple[str, ...] = tuple(sorted(STABLE_BASES | {"USD1"}))
    excluded_quotes: tuple[str, ...] = ("USDC", "FDUSD", "BUSD", "TUSD", "USDP", "DAI", "PAX")


@dataclass(frozen=True)
class UniversePick:
    symbols: list[str]
    ranked: list[dict[str, Any]]
    generated_at: str
    cfg: dict[str, Any]


class UniverseSelector:
    def __init__(self, exchange: Any, cfg: UniverseConfig | None = None) -> None:
        """exchange is expected to be a ccxt-like object."""
        self.exchange = exchange
        self.cfg = cfg or UniverseConfig()
        self.regime = RegimeDetector()

    def _iter_usdt_symbols(self) -> list[str]:
        markets: dict[str, Any] = getattr(self.exchange, "markets", {}) or {}
        excluded_symbols = {normalize_symbol(s) for s in self.cfg.excluded_symbols}
        excluded_bases = {str(s).upper() for s in self.cfg.excluded_bases}
        excluded_quotes = {str(s).upper() for s in self.cfg.excluded_quotes}
        out: list[str] = []
        for sym, m in markets.items():
            try:
                s = normalize_symbol(sym)
                if not s.endswith("/USDT"):
                    continue
                base, quote = s.split("/", 1)
                base = (m.get("base") or base).upper()
                quote = (m.get("quote") or quote).upper()
                if s in excluded_symbols:
                    continue
                if base in excluded_bases:
                    continue
                if quote in excluded_quotes:
                    continue
                if any(tag in base for tag in ("UP", "DOWN", "BULL", "BEAR")):
                    continue
                if m.get("active") is False:
                    continue
                out.append(s)
            except Exception:
                continue
        return sorted(set(out))

    @staticmethod
    def _compute_spread_pct(t: dict[str, Any]) -> float | None:
        bid = t.get("bid")
        ask = t.get("ask")
        if bid is None or ask is None:
            return None
        bid = float(bid)
        ask = float(ask)
        if bid <= 0 or ask <= 0 or ask < bid:
            return None
        mid = (bid + ask) / 2.0
        return (ask - bid) / mid * 100.0

    def _fetch_ohlcv_bars(self, symbol: str, timeframe: str, limit: int) -> list[list[float]]:
        return self.exchange.fetch_ohlcv(symbol, timeframe, None, limit)

    def rebuild(self) -> UniversePick:
        cfg = self.cfg
        anchors = [normalize_symbol(s) for s in cfg.anchors]

        syms = self._iter_usdt_symbols()
        if not syms:
            raise RuntimeError("no_usdt_symbols")

        # Binance "symbols" param formatı yüzünden slash'lı unified symbol listesi gönderme.
        # Tüm tickers'ı çek, lokal filtrele.
        tickers: dict[str, Any] = self.exchange.fetch_tickers()

        rows: list[dict[str, Any]] = []
        for s in syms:
            t = tickers.get(s)
            if not t:
                # fallback: market id üzerinden arama
                try:
                    m = self.exchange.market(s) if hasattr(self.exchange, "market") else None
                    mid = m.get("id") if isinstance(m, dict) else None
                    if mid:
                        # bazı implementasyonlar key olarak id kullanabilir
                        t = tickers.get(mid)
                except Exception:
                    t = None
            t = t or {}
            qv = t.get("quoteVolume")
            if qv is None:
                continue
            quote_vol = float(qv)
            spread_pct = self._compute_spread_pct(t)
            if spread_pct is None:
                continue
            if quote_vol < cfg.min_quote_volume_usd:
                continue
            if spread_pct > cfg.max_spread_pct:
                continue
            rows.append({"symbol": s, "quote_vol": quote_vol, "spread_pct": spread_pct})

        if not rows:
            raise RuntimeError("no_symbols_after_liquidity_filters")

        df = pd.DataFrame(rows)
        df = df.sort_values("quote_vol", ascending=False).head(cfg.top_by_volume_for_trend_eval)

        # liquidity score (log volume + spread)
        df["vol_log"] = df["quote_vol"].apply(lambda x: math.log10(max(1.0, float(x))))
        vmin, vmax = float(df["vol_log"].min()), float(df["vol_log"].max())
        df["vol_norm"] = df["vol_log"].apply(lambda x: 1.0 if vmax == vmin else _clamp((float(x) - vmin) / (vmax - vmin)))
        df["spread_score"] = df["spread_pct"].apply(lambda x: _clamp(1.0 - (float(x) / cfg.max_spread_pct)))
        df["liquidity_score"] = 0.7 * df["vol_norm"] + 0.3 * df["spread_score"]

        ranked: list[dict[str, Any]] = []
        for _, r in df.iterrows():
            s = str(r["symbol"])
            try:
                ohlcv = self._fetch_ohlcv_bars(s, cfg.timeframe, cfg.trend_lookback_bars)
                if not ohlcv or len(ohlcv) < 250:
                    continue
                highs = [x[2] for x in ohlcv]
                lows = [x[3] for x in ohlcv]
                closes = [x[4] for x in ohlcv]

                reg = self.regime.detect(highs, lows, closes)
                adx = reg.adx or 0.0
                er = reg.er or 0.0
                atr_pct = reg.atr_pct or 0.0

                # Chop proxy: EMA200 cross count (last 250)
                s_close = pd.Series(closes[-250:], dtype="float64")
                ema200 = s_close.ewm(span=200, adjust=False).mean()
                side = (s_close - ema200).apply(lambda x: 1 if x >= 0 else -1)
                crossings = int((side != side.shift(1)).sum() - 1)
                chop_penalty = _clamp(crossings / 20.0)

                adx_norm = _clamp(adx / 40.0)
                er_norm = _clamp(er)
                trend_score = _clamp(0.5 * adx_norm + 0.5 * er_norm - 0.5 * chop_penalty)

                # Vol quality: ideal around 4% ATR
                vol_quality = _clamp(1.0 - abs(atr_pct - 0.04) / 0.04)

                final_score = _clamp(
                    0.4 * float(r["liquidity_score"]) + 0.4 * trend_score + 0.2 * vol_quality
                )

                ranked.append(
                    {
                        "symbol": s,
                        "final_score": float(final_score),
                        "liquidity_score": float(r["liquidity_score"]),
                        "trend_score": float(trend_score),
                        "vol_quality": float(vol_quality),
                        "quote_vol": float(r["quote_vol"]),
                        "spread_pct": float(r["spread_pct"]),
                        "adx": float(adx) if reg.adx is not None else None,
                        "er": float(er) if reg.er is not None else None,
                        "atr_pct": float(atr_pct) if reg.atr_pct is not None else None,
                        "regime": reg.regime,
                        "regime_reason": reg.reason,
                        "chop_crossings_250": crossings,
                    }
                )
            except Exception as e:
                logger.warning("universe_trend_eval_failed symbol=%s err=%s", s, str(e)[:200])
                continue

        ranked.sort(key=lambda x: x["final_score"], reverse=True)
        dynamic: list[str] = []
        for row in ranked:
            s = normalize_symbol(row["symbol"])
            if s in anchors:
                continue
            dynamic.append(s)
            if len(dynamic) >= cfg.dynamic_n:
                break

        symbols = list(dict.fromkeys(anchors + dynamic))
        ts = datetime.now(tz=UTC_TZ).isoformat(timespec="seconds")
        return UniversePick(
            symbols=symbols,
            ranked=ranked,
            generated_at=ts,
            cfg={
                "anchors": list(anchors),
                "dynamic_n": cfg.dynamic_n,
                "min_quote_volume_usd": cfg.min_quote_volume_usd,
                "max_spread_pct": cfg.max_spread_pct,
                "timeframe": cfg.timeframe,
                "trend_lookback_bars": cfg.trend_lookback_bars,
                "top_by_volume_for_trend_eval": cfg.top_by_volume_for_trend_eval,
            },
        )


def compute_next_rebuild_at(days: int = 14) -> str:
    return (datetime.now(tz=UTC_TZ) + timedelta(days=int(days))).isoformat(timespec="seconds")
