# File: backend/tests/test_research_endpoints.py
from __future__ import annotations

import os
import tempfile
import unittest

from backend.research_store import ResearchStore, OhlcvRow
from backend.research import ResearchEngine


class TestResearchEngine(unittest.TestCase):
    def test_ingest_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "research.db")
            store = ResearchStore(db_path=db_path)

            # fake fetch: 300 candles
            def fake_fetch(symbol: str, tf: str, since: int | None, limit: int) -> list[list]:
                base = 1_700_000_000_000 if since is None else since
                out = []
                for i in range(300):
                    ts = base + i * 3_600_000
                    out.append([ts, 100 + i, 101 + i, 99 + i, 100 + i, 1000.0])
                return out

            eng = ResearchEngine(store=store, fetch_ohlcv=fake_fetch, sleep_s=0.0)
            res = eng.ingest_symbol("BTC/USDT", "1h", days_back=365, limit=1000)
            self.assertGreaterEqual(res.inserted, 1)

            rep = eng.analyze_symbol("BTC/USDT", "1h", since_days=3650)
            self.assertNotIn("error", rep)
            self.assertIn("regime_distribution", rep)


if __name__ == "__main__":
    unittest.main()