from __future__ import annotations

import unittest
import logging

from backend.trader import Trader, TradingPausedError


class DummyDB:
    def __init__(self) -> None:
        self.enabled = True

    def set_bot_enabled(self, enabled: bool) -> None:
        self.enabled = enabled


class DummyNotifier:
    def __init__(self) -> None:
        self.enabled = True
        self.calls: list[tuple[str, str]] = []

    def notify_paused_alert(self, reason: str, detail: str = "") -> None:
        self.calls.append((reason, detail))


class TraderFailClosedPauseTests(unittest.TestCase):
    def setUp(self) -> None:
        logging.disable(logging.CRITICAL)

    def tearDown(self) -> None:
        logging.disable(logging.NOTSET)

    def test_pause_helper_disables_bot_and_notifies(self) -> None:
        trader = Trader.__new__(Trader)
        trader.db = DummyDB()
        trader.notifier = DummyNotifier()

        with self.assertRaises(TradingPausedError):
            trader._pause_after_trade_persist_failure(
                symbol="BNB/USDT",
                side="BUY",
                error=RuntimeError("simulated db failure"),
            )

        self.assertFalse(trader.db.enabled)
        self.assertTrue(trader.notifier.calls)
        self.assertEqual(trader.notifier.calls[0][0], "DB persistence failed")


if __name__ == "__main__":
    unittest.main()
