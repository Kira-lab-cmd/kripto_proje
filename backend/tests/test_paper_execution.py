# File: backend/tests/test_paper_execution.py
from __future__ import annotations

import os
import tempfile
import unittest

from backend.database import Database
from backend.paper_execution import PaperExecutor, PaperSlipConfig


class TestPaperExecution(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.tmp.close()
        self.db = Database(db_path=self.tmp.name)
        self.db.connect()
        self.db.reset_paper_balances(usdt=200.0, clear_positions=True, clear_trades=True, clear_paper_orders=True)

        self.exec = PaperExecutor(
            db=self.db,
            commission_rate=0.001,
            slip_cfg=PaperSlipConfig(base_slippage_bps=2.0, jitter_bps=1.0),
        )

    def tearDown(self) -> None:
        try:
            self.db.close_db()
        finally:
            try:
                os.unlink(self.tmp.name)
            except Exception:
                pass

    def test_idempotency(self) -> None:
        symbol = "BTC/USDT"
        key = "deadbeef" * 8  # 64 hex chars
        mid = 100.0
        amount = 1.0

        before = self.db.get_paper_balance("USDT")
        order1 = self.exec.execute_market_order(
            symbol=symbol,
            side="BUY",
            amount=amount,
            mid_price=mid,
            idempotency_key=key,
            reason="UNITTEST",
        )
        after1 = self.db.get_paper_balance("USDT")
        self.assertLess(after1, before)

        order2 = self.exec.execute_market_order(
            symbol=symbol,
            side="BUY",
            amount=amount,
            mid_price=mid,
            idempotency_key=key,
            reason="UNITTEST",
        )
        after2 = self.db.get_paper_balance("USDT")

        self.assertEqual(order1["id"], order2["id"])
        self.assertAlmostEqual(after1, after2, places=10)

    def test_fee_and_slippage_applied(self) -> None:
        symbol = "BTC/USDT"
        key = "c0ffee" * 10 + "00"
        key = (key + "0" * 64)[:64]
        mid = 100.0
        amount = 1.0

        order = self.exec.execute_market_order(
            symbol=symbol,
            side="BUY",
            amount=amount,
            mid_price=mid,
            idempotency_key=key,
            reason="UNITTEST",
        )
        exec_price = float(order["average"])
        self.assertGreater(exec_price, mid)
        self.assertGreater(float(order["fee"]), 0.0)


if __name__ == "__main__":
    unittest.main()