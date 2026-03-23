# File: backend/tests/test_risk_guards.py
from __future__ import annotations

from typing import Optional
from backend.risk_guards import (
    resolve_daily_loss_limit_usdt,
    compute_open_positions_unrealized,
    check_daily_loss_and_kill_switch,
)

class FakeDB:
    def __init__(self):
        self.enabled = True
        self.positions = []
        self.realized_today = 0.0

    def get_open_positions(self):
        return list(self.positions)

    def get_today_realized_pnl(self) -> float:
        return float(self.realized_today)

    def set_bot_enabled(self, v: bool):
        self.enabled = bool(v)

    def get_bot_enabled(self):
        return self.enabled


def test_resolve_abs_limit(monkeypatch):
    monkeypatch.setenv("DAILY_LOSS_LIMIT_USDT", "10")
    monkeypatch.setenv("DAILY_LOSS_LIMIT_PCT", "1")
    assert resolve_daily_loss_limit_usdt(1000) == 10.0

def test_resolve_pct_limit(monkeypatch):
    monkeypatch.delenv("DAILY_LOSS_LIMIT_USDT", raising=False)
    monkeypatch.setenv("DAILY_LOSS_LIMIT_PCT", "1")  # 1%
    assert abs(resolve_daily_loss_limit_usdt(1000) - 10.0) < 1e-9

def test_unrealized_long():
    db = FakeDB()
    db.positions = [{"symbol":"ETH/USDT","entry_price":100.0,"amount":2.0,"side":"LONG"}]
    def px(_: str) -> Optional[float]:
        return 110.0
    assert compute_open_positions_unrealized(db, px) == 20.0

def test_kill_switch_triggers():
    db = FakeDB()
    db.realized_today = -6.0
    db.positions = [{"symbol":"ETH/USDT","entry_price":100.0,"amount":1.0,"side":"LONG"}]
    def px(_: str) -> Optional[float]:
        return 95.0  # unrealized -5
    # equity 1000, pct 1% => 10
    import os
    os.environ["DAILY_LOSS_LIMIT_USDT"] = ""
    os.environ["DAILY_LOSS_LIMIT_PCT"] = "1"
    triggered = check_daily_loss_and_kill_switch(db=db, price_fn=px, equity_usdt=1000.0)
    assert triggered is True
    assert db.enabled is False