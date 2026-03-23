from __future__ import annotations

import os
import unittest
from contextlib import contextmanager
from types import SimpleNamespace

from backend.strategy import TradingStrategy


def _make_flat_ohlcv(n: int = 250, px: float = 100.0):
    # [timestamp, open, high, low, close, volume]
    out = []
    ts = 1700000000000
    for i in range(n):
        out.append([ts + i * 60_000, px, px * 1.001, px * 0.999, px, 1000.0])
    return out


@contextmanager
def _patched_environ(env: dict[str, str]):
    old = dict(os.environ)
    try:
        os.environ.update(env)
        yield
    finally:
        os.environ.clear()
        os.environ.update(old)


class StrategyCalibrationTests(unittest.TestCase):
    @staticmethod
    def _soft_trend_result():
        return SimpleNamespace(
            regime="SOFT_TREND",
            confidence=0.75,
            reason="stub soft trend",
            adx=38.0,
            er=0.30,
        )

    def test_calibration_mode_shifts_thresholds_case_1(self) -> None:
        self._run_case(calib_offset=-0.5)

    def test_calibration_mode_shifts_thresholds_case_2(self) -> None:
        self._run_case(calib_offset=-1.0)

    def _run_case(self, calib_offset: float) -> None:
        ohlcv = _make_flat_ohlcv()
        profile = {
            "buy_threshold": 3.0,
            "sell_threshold": -3.0,
            "min_volume_ratio": 0.5,
            "min_atr_pct": 0.002,
            "max_atr_pct": 0.06,
            "downtrend_buy_penalty": 2.0,
            "uptrend_buy_boost": 1.0,
            "risk_mult": 1.0,
        }

        # Baseline (no calibration): capture the effective buy threshold.
        with _patched_environ({"CHOP_TH_BOOST": "0.25", "CHOP_SCORE_PENALTY": "0.25"}):
            base_strat = TradingStrategy()
            base_strat.regime = SimpleNamespace(detect=lambda **kwargs: self._soft_trend_result())
            base_res = base_strat.get_signal(
                ohlcv,
                sentiment_score=0.0,
                symbol="BTC/USDT",
                profile=profile,
                trend_dir_1h="UP",
            )
            base_th = float((base_res.get("effective_thresholds") or {}).get("buy"))

        # Calibrated: buy threshold should not be harder than baseline when offset is negative.
        with _patched_environ(
            {
                "CALIBRATION_MODE": "1",
                "CALIB_BUY_TH_OFFSET": str(calib_offset),
                "CHOP_TH_BOOST": "0.25",
                "CHOP_SCORE_PENALTY": "0.25",
            }
        ):
            strat = TradingStrategy()
            strat.regime = SimpleNamespace(detect=lambda **kwargs: self._soft_trend_result())
            res = strat.get_signal(
                ohlcv,
                sentiment_score=0.0,
                symbol="BTC/USDT",
                profile=profile,
                trend_dir_1h="UP",
            )

            th = res.get("effective_thresholds", {})
            self.assertIn("buy", th)
            calibrated_buy_th = float(th["buy"])
            self.assertLessEqual(calibrated_buy_th, base_th)


if __name__ == "__main__":
    unittest.main()
