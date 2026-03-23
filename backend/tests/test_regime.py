# File: backend/tests/test_regime.py
from __future__ import annotations

import unittest

from backend.regime import RegimeDetector, RegimeConfig


def _make_ohlc_from_closes(closes: list[float]) -> tuple[list[float], list[float], list[float]]:
    highs = [c * 1.002 for c in closes]
    lows = [c * 0.998 for c in closes]
    return highs, lows, closes


class TestRegimeDetector(unittest.TestCase):
    def test_trend_regime_on_monotonic_series(self) -> None:
        closes = [float(i) for i in range(1, 400)]
        highs, lows, closes = _make_ohlc_from_closes(closes)

        det = RegimeDetector(
            RegimeConfig(
                adx_period=14,
                er_period=30,
                high_vol_atr_pct=0.50,  # disable high-vol gating for test
                trend_adx_min=15.0,     # make trend easier in synthetic data
                trend_er_min=0.60,
                chop_adx_max=18.0,
                chop_er_max=0.25,
            )
        )
        res = det.detect(highs, lows, closes)
        self.assertEqual(res.regime, "TREND")
        self.assertIsNotNone(res.er)
        self.assertGreaterEqual(float(res.er), 0.70)

    def test_chop_regime_on_alternating_series(self) -> None:
        closes = []
        base = 100.0
        for i in range(400):
            closes.append(base + (1.0 if i % 2 == 0 else -1.0))
        highs, lows, closes = _make_ohlc_from_closes(closes)

        det = RegimeDetector(
            RegimeConfig(
                adx_period=14,
                er_period=30,
                high_vol_atr_pct=0.50,
                trend_adx_min=30.0,
                trend_er_min=0.60,
                chop_adx_max=25.0,
                chop_er_max=0.30,
            )
        )
        res = det.detect(highs, lows, closes)
        self.assertEqual(res.regime, "CHOP")
        self.assertIsNotNone(res.er)
        self.assertLessEqual(float(res.er), 0.35)


if __name__ == "__main__":
    unittest.main()