from __future__ import annotations

import os
import tempfile
import time
import unittest

from backend.core.correlation import CorrelationConfig, CorrelationService
from backend.research_store import OhlcvRow, ResearchStore


class _NullLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None


class CorrelationOhlcvRowTests(unittest.TestCase):
    def test_correlation_service_accepts_ohlcvrow_objects(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "research.db")
            store = ResearchStore(db_path=db_path)
            store.init_schema()

            # Insert synthetic data
            rows = []
            # Place candles within lookback window
            ts0 = int(time.time() * 1000) - (400 * 3_600_000)
            for i in range(300):
                ts = ts0 + i * 3_600_000
                # two highly correlated series
                rows.append(OhlcvRow("AAA/USDT", "1h", ts, 100 + i, 101 + i, 99 + i, 100 + i, 1000.0))
                rows.append(OhlcvRow("BBB/USDT", "1h", ts, 200 + i, 201 + i, 199 + i, 200 + i, 1000.0))
            store.upsert_rows(rows)

            svc = CorrelationService(store, CorrelationConfig(timeframe="1h", lookback_days=365, min_bars=200), _NullLogger())

            st = svc._compute_sync(["AAA/USDT", "BBB/USDT"])  # sync path for unit test
            self.assertIn(("AAA/USDT", "BBB/USDT"), st.matrix)
            self.assertGreater(st.matrix[("AAA/USDT", "BBB/USDT")], 0.9)


if __name__ == "__main__":
    unittest.main()
