from __future__ import annotations

import os
import unittest

from backend.notifier import TelegramNotifier


class TelegramNotifierTests(unittest.TestCase):
    def test_disabled_notifier_drops_sync_notifications_without_warning(self) -> None:
        prev_enabled = os.environ.get("TELEGRAM_ENABLED")
        prev_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        prev_chat = os.environ.get("TELEGRAM_CHAT_ID")
        os.environ["TELEGRAM_ENABLED"] = "0"
        os.environ["TELEGRAM_BOT_TOKEN"] = "token"
        os.environ["TELEGRAM_CHAT_ID"] = "chat"
        try:
            notifier = TelegramNotifier()
            with self.assertNoLogs("backend.notifier", level="WARNING"):
                notifier.notify_paused_alert("TEST_DISABLED", "detail")
                notifier.notify_trade_alert(symbol="BTC/USDT", side="BUY", amount=1.0, price=100.0)
            self.assertFalse(notifier.enabled)
        finally:
            if prev_enabled is None:
                os.environ.pop("TELEGRAM_ENABLED", None)
            else:
                os.environ["TELEGRAM_ENABLED"] = prev_enabled

            if prev_token is None:
                os.environ.pop("TELEGRAM_BOT_TOKEN", None)
            else:
                os.environ["TELEGRAM_BOT_TOKEN"] = prev_token

            if prev_chat is None:
                os.environ.pop("TELEGRAM_CHAT_ID", None)
            else:
                os.environ["TELEGRAM_CHAT_ID"] = prev_chat


if __name__ == "__main__":
    unittest.main()
