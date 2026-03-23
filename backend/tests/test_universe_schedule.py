from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from backend.database import Database
from backend.main import _get_universe_rebuild_due_state


class UniverseScheduleTests(unittest.TestCase):
    def test_database_initializes_next_rebuild_at(self) -> None:
        prev = os.environ.get("UNIVERSE_REBUILD_DAYS")
        os.environ["UNIVERSE_REBUILD_DAYS"] = "14"
        temp_dir = tempfile.mkdtemp()
        try:
            db_path = os.path.join(temp_dir, "test_trading_bot.db")
            db = Database(db_path)
            try:
                db.connect()
                st = db.get_universe_state()
            finally:
                db.close_db()

            self.assertTrue(st["next_rebuild_at"])
            updated_at = datetime.fromisoformat(str(st["updated_at"]))
            next_rebuild_at = datetime.fromisoformat(str(st["next_rebuild_at"]))
            self.assertGreater(next_rebuild_at, updated_at)
        finally:
            if prev is None:
                os.environ.pop("UNIVERSE_REBUILD_DAYS", None)
            else:
                os.environ["UNIVERSE_REBUILD_DAYS"] = prev
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_due_state_initializes_missing_schedule_without_due(self) -> None:
        due, init_next = _get_universe_rebuild_due_state({"symbols": ["BTC/USDT"], "next_rebuild_at": None})
        self.assertFalse(due)
        self.assertTrue(init_next)

    def test_due_state_waits_until_next_rebuild_at(self) -> None:
        now_utc = datetime(2026, 3, 3, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        due_future, init_future = _get_universe_rebuild_due_state(
            {"next_rebuild_at": "2026-03-03T12:00:01"},
            now_utc=now_utc,
        )
        self.assertFalse(due_future)
        self.assertIsNone(init_future)

        due_now, init_now = _get_universe_rebuild_due_state(
            {"next_rebuild_at": "2026-03-03T12:00:00"},
            now_utc=now_utc,
        )
        self.assertTrue(due_now)
        self.assertIsNone(init_now)


if __name__ == "__main__":
    unittest.main()
