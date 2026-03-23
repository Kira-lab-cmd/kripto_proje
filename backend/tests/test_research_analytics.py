# File: backend/tests/test_research_analytics.py
from __future__ import annotations

import unittest
import tempfile
import os

from backend.research_store import ResearchStore, OhlcvRow
from backend.research import ResearchEngine


class TestResearchAnalytics(unittest.TestCase):
    def test_analyze_symbol_basic(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "test.db")
            store = ResearchStore(db_path=db_path)
            store.init_schema()

            # create synthetic OHLCV (monotonic up)
            rows = []
            ts = 1_700_000_000_000
            for i in range(300):
                price = 100.0 + i * 0.5
                rows.append(
                    OhlcvRow(
                        symbol="BTC/USDT",
                        timeframe="1h",
                        ts_ms=ts + i * 3_600_000,
                        open=price,
                        high=price * 1.01,
                        low=price * 0.99,
                        close=price,
                        volume=1000.0,
                    )
                )
            store.upsert_ohlcv_rows(rows)

            eng = ResearchEngine(store=store, fetch_ohlcv_fn=lambda *a, **k: [])
            rep = eng.analyze_symbol("BTC/USDT", "1h", since_days=9999)

            self.assertNotIn("error", rep)
            self.assertEqual(rep["symbol"], "BTC/USDT")
            self.assertGreater(rep["rows"], 200)
            self.assertIn("regime_distribution", rep)

    def test_analyze_universe_corr(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "test.db")
            store = ResearchStore(db_path=db_path)
            store.init_schema()

            ts = 1_700_000_000_000
            for sym in ["BTC/USDT", "ETH/USDT"]:
                rows = []
                for i in range(300):
                    price = 100.0 + i * (1.0 if sym.startswith("BTC") else 0.8)
                    rows.append(
                        OhlcvRow(
                            symbol=sym,
                            timeframe="1h",
                            ts_ms=ts + i * 3_600_000,
                            open=price,
                            high=price * 1.01,
                            low=price * 0.99,
                            close=price,
                            volume=1000.0,
                        )
                    )
                store.upsert_ohlcv_rows(rows)

            eng = ResearchEngine(store=store, fetch_ohlcv_fn=lambda *a, **k: [])
            rep = eng.analyze_universe(["BTC/USDT", "ETH/USDT"], "1h", since_days=9999)
            self.assertIn("corr", rep)
            self.assertIn("BTC/USDT", rep["corr"])


if __name__ == "__main__":
    unittest.main()