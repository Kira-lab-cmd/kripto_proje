from __future__ import annotations

import unittest
from types import SimpleNamespace

from backend.strategy import TradingStrategy


def _make_flat_ohlcv(n: int = 250, px: float = 100.0):
    out = []
    ts = 1700000000000
    for i in range(n):
        out.append([ts + i * 60_000, px, px * 1.001, px * 0.999, px, 1000.0])
    return out


class StrategyRegimeHoldTests(unittest.TestCase):
    def test_chop_regime_forces_hold(self) -> None:
        strat = TradingStrategy()
        strat.regime = SimpleNamespace(
            detect=lambda **kwargs: SimpleNamespace(
                regime="CHOP",
                confidence=0.91,
                reason="stub chop",
                adx=12.0,
                er=0.08,
            )
        )

        res = strat.get_signal(
            _make_flat_ohlcv(),
            sentiment_score=0.75,
            symbol="BTC/USDT",
            trend_dir_1h="UP",
        )

        self.assertEqual(res["regime"], "CHOP")
        self.assertEqual(res["signal"], "HOLD")
        self.assertIn("CHOP force hold", str(res["reason"]))


if __name__ == "__main__":
    unittest.main()
