from __future__ import annotations

import json
import os
import tempfile
import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.database import Database, utc_naive_iso_now
from backend.middleware.error_handler import register_exception_handlers
from backend.risk_limits import resolve_daily_loss_limit_usdt
from backend.trader import TradingPausedError


class DailyLossLimitPauseTests(unittest.TestCase):
    def test_daily_loss_limit_triggers_trading_paused_503(self) -> None:
        os.environ["DAILY_MAX_LOSS_USDT"] = "0"
        os.environ["DAILY_MAX_LOSS_PCT"] = "0.02"  # 2%

        # NOTE: On Windows, NamedTemporaryFile keeps an open handle and SQLite may fail to open it.
        # Use a temp directory and a normal path instead.
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "test_trading_bot.db")
            with Database(db_path) as db:
                db.connect()
                db.reset_paper_balances(usdt=200.0, clear_positions=True, clear_trades=True)

                # two losing trades today => -5.0 USDT
                ts = utc_naive_iso_now(timespec="seconds")
                db.add_trade(
                    timestamp=ts,
                    symbol="BTC/USDT",
                    side="SELL",
                    amount=0.001,
                    price=50000,
                    cost=50,
                    fee=0.0,
                    realized_pnl=-2.5,
                )
                db.add_trade(
                    timestamp=ts,
                    symbol="ETH/USDT",
                    side="SELL",
                    amount=0.01,
                    price=3000,
                    cost=30,
                    fee=0.0,
                    realized_pnl=-2.5,
                )

                app = FastAPI()
                register_exception_handlers(app)

                @app.get("/probe")
                def probe():
                    equity = 200.0
                    limit = resolve_daily_loss_limit_usdt(equity_usdt=equity)
                    realized = float(db.get_today_realized_pnl())
                    if limit > 0 and realized <= -abs(limit):
                        raise TradingPausedError("DAILY_LOSS_LIMIT")
                    return {"ok": True, "realized": realized, "limit": limit}

                client = TestClient(app)
                resp = client.get("/probe")
                self.assertEqual(resp.status_code, 503)
                body = json.loads(resp.text)
                self.assertEqual(body["error"]["code"], "TRADING_PAUSED")
                self.assertEqual(body["error"]["message"], "DAILY_LOSS_LIMIT")


if __name__ == "__main__":
    unittest.main()
