# File: backend/research.py
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Callable

import pandas as pd

from .regime import RegimeConfig, RegimeDetector
from .research_store import OhlcvRow, ResearchStore
from .utils_symbols import normalize_symbol


@dataclass(frozen=True)
class IngestResult:
    symbol: str
    timeframe: str
    inserted: int
    last_ts_ms: int | None


class ResearchEngine:
    """
    Ingest OHLCV from exchange into local store, then compute behavior profiles.
    """

    def __init__(
        self,
        store: ResearchStore,
        fetch_ohlcv: Callable[[str, str, int | None, int], list[list]] | None = None,
        *,
        fetch_ohlcv_fn: Callable[[str, str, int | None, int], list[list]] | None = None,
        sleep_s: float = 0.20,
    ) -> None:
        self.store = store
        self.fetch_ohlcv = fetch_ohlcv or fetch_ohlcv_fn
        if self.fetch_ohlcv is None:
            raise TypeError("ResearchEngine requires fetch_ohlcv (or fetch_ohlcv_fn)")
        self.sleep_s = float(max(0.0, sleep_s))

        self.regime = RegimeDetector(
            RegimeConfig(
                adx_period=14,
                er_period=20,
                high_vol_atr_pct=0.060,
                trend_adx_min=23.0,
                trend_er_min=0.40,
                chop_adx_max=18.0,
                chop_er_max=0.25,
            )
        )

    def init_schema(self) -> None:
        self.store.init_schema()

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _ms_per_timeframe(timeframe: str) -> int:
        tf = timeframe.strip().lower()
        if tf.endswith("m"):
            return int(tf[:-1]) * 60_000
        if tf.endswith("h"):
            return int(tf[:-1]) * 3_600_000
        if tf.endswith("d"):
            return int(tf[:-1]) * 86_400_000
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    def ingest_symbol(self, symbol: str, timeframe: str, days_back: int, limit: int = 1000) -> IngestResult:
        self.init_schema()

        sym = normalize_symbol(symbol)
        tf = timeframe.strip()

        latest = self.store.latest_ts(sym, tf)
        if latest is None:
            since = self._now_ms() - int(days_back) * 86_400_000
        else:
            since = latest + self._ms_per_timeframe(tf)

        inserted_total = 0
        last_ts: int | None = None

        # pagination guard: prevents infinite loops
        for _ in range(2500):
            batch = self.fetch_ohlcv(sym, tf, since, int(limit))
            if not batch:
                break

            rows: list[OhlcvRow] = []
            for c in batch:
                ts_ms = int(c[0])
                rows.append(
                    OhlcvRow(
                        symbol=sym,
                        timeframe=tf,
                        ts_ms=ts_ms,
                        open=float(c[1]),
                        high=float(c[2]),
                        low=float(c[3]),
                        close=float(c[4]),
                        volume=float(c[5]),
                    )
                )
                last_ts = ts_ms

            inserted_total += self.store.upsert_rows(rows)

            if last_ts is None:
                break

            since = last_ts + self._ms_per_timeframe(tf)

            if len(batch) < int(limit):
                break

            if self.sleep_s > 0:
                time.sleep(self.sleep_s)

        return IngestResult(symbol=sym, timeframe=tf, inserted=int(inserted_total), last_ts_ms=last_ts)

    @staticmethod
    def _max_drawdown(equity: pd.Series) -> float:
        if equity.empty:
            return 0.0
        peak = equity.cummax()
        dd = (equity / peak) - 1.0
        return float(dd.min())

    def analyze_symbol(self, symbol: str, timeframe: str, since_days: int) -> dict:
        sym = normalize_symbol(symbol)
        tf = timeframe.strip()

        since_ms = self._now_ms() - int(since_days) * 86_400_000
        rows = self.store.load(sym, tf, since_ms=since_ms)

        if len(rows) < 250:
            return {"symbol": sym, "timeframe": tf, "error": "insufficient_local_history", "rows": len(rows)}

        df = pd.DataFrame([r.__dict__ for r in rows]).sort_values("ts_ms").reset_index(drop=True)
        df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
        df["ret"] = df["close"].pct_change().fillna(0.0)
        df["equity"] = (1.0 + df["ret"]).cumprod()

        ms_tf = self._ms_per_timeframe(tf)
        candles_per_day = max(1, int(86_400_000 // ms_tf))
        ann_factor = math.sqrt(365.0 * candles_per_day)

        vol_ann = float(df["ret"].std()) * ann_factor
        ret_ann = float(df["ret"].mean()) * (365.0 * candles_per_day)
        mdd = self._max_drawdown(df["equity"])

        df["hour"] = df["ts"].dt.hour
        df["dow"] = df["ts"].dt.dayofweek

        hour_ret = df.groupby("hour")["ret"].mean().round(8).to_dict()
        dow_ret = df.groupby("dow")["ret"].mean().round(8).to_dict()

        highs = df["high"].astype(float).tolist()
        lows = df["low"].astype(float).tolist()
        closes = df["close"].astype(float).tolist()

        regimes = {"TREND": 0, "CHOP": 0, "HIGH_VOL": 0, "UNKNOWN": 0}
        transitions: dict[str, int] = {}

        sample_step = max(1, len(df) // 500)
        last_reg: str | None = None

        for i in range(220, len(df), sample_step):
            res = self.regime.detect(highs[: i + 1], lows[: i + 1], closes[: i + 1])
            regimes[res.regime] = regimes.get(res.regime, 0) + 1
            if last_reg is not None:
                k = f"{last_reg}->{res.regime}"
                transitions[k] = transitions.get(k, 0) + 1
            last_reg = res.regime

        total = max(1, sum(regimes.values()))
        regime_distribution = {k: round(v / total, 4) for k, v in regimes.items()}

        return {
            "symbol": sym,
            "timeframe": tf,
            "rows": int(len(df)),
            "period_days": int(since_days),
            "return_ann": round(ret_ann, 6),
            "vol_ann": round(vol_ann, 6),
            "max_drawdown": round(mdd, 6),
            "seasonality_hour_ret": hour_ret,
            "seasonality_dow_ret": dow_ret,
            "regime_distribution": regime_distribution,
            "regime_transitions": transitions,
        }

    def analyze_universe(self, symbols: list[str], timeframe: str, since_days: int) -> dict:
        tf = timeframe.strip()
        reports = [self.analyze_symbol(s, tf, since_days) for s in symbols]

        frames = []
        since_ms = self._now_ms() - int(since_days) * 86_400_000
        for r in reports:
            if "error" in r:
                continue
            rows = self.store.load(r["symbol"], tf, since_ms=since_ms)
            df = pd.DataFrame([x.__dict__ for x in rows]).sort_values("ts_ms")
            df["ret"] = df["close"].pct_change().fillna(0.0)
            frames.append(df[["ts_ms", "ret"]].rename(columns={"ret": r["symbol"]}).set_index("ts_ms"))

        corr = {}
        if len(frames) >= 2:
            merged = pd.concat(frames, axis=1, join="inner").fillna(0.0)
            corr = merged.corr().round(4).to_dict()

        return {"timeframe": tf, "since_days": int(since_days), "symbols": symbols, "reports": reports, "corr": corr}