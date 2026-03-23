# File: backend/tests/test_replay_engine_smoke.py
import os
import tempfile
import unittest
from dataclasses import asdict
from datetime import datetime, timezone, timedelta

from backend.execution_model import DefaultExecutionModel
from backend.portfolio_risk import PortfolioRiskEngine, RiskLimits
from backend.replay import _to_payload
from backend.replay_engine import ReplayEngine, ReplayConfig
from backend.research_store import ResearchStore, OhlcvRow
from backend.strategy import TradingStrategy

UTC = timezone.utc


class _AlwaysBuyStrategy:
    def get_signal(self, ohlcv, sentiment_score, symbol, trend_dir_1h=None):
        px = float(ohlcv[-1][4])
        return {
            "signal": "BUY",
            "score": 3.5,
            "current_price": px,
            "stop_loss": px * 0.99,
            "take_profit": px * 1.02,
            "reason": "smoke_always_buy",
            "regime": "TREND",
            "regime_conf": 0.8,
            "adx": 25.0,
            "er": 0.4,
            "atr_pct": 0.006,
            "vol_ratio": 1.1,
            "dir_1h": trend_dir_1h or "UNKNOWN",
            "is_uptrend": True,
            "ema200": px * 0.95,
            "atr": px * 0.006,
            "effective_thresholds": {"buy": 2.0, "sell": -2.0},
        }


class ReplayEngineSmokeTests(unittest.TestCase):
    def test_replay_runs_with_synthetic_data(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            store = ResearchStore(db_path)
            store.init_schema()

            symbol = "BTC/USDT"
            # Build synthetic 15m uptrend with enough bars
            start = datetime(2025, 1, 1, tzinfo=UTC)
            rows15 = []
            price = 100.0
            for i in range(60):
                ts = int((start + timedelta(minutes=15*i)).timestamp() * 1000)
                o = price
                c = price * 1.001
                h = max(o, c) * 1.0005
                l = min(o, c) * 0.9995
                v = 1000.0
                rows15.append(OhlcvRow(symbol, "15m", ts, o, h, l, c, v))
                price = c

            # Build synthetic 1h bars
            rows1h = []
            price = 100.0
            for i in range(40):
                ts = int((start + timedelta(hours=i)).timestamp() * 1000)
                o = price
                c = price * 1.002
                h = max(o, c) * 1.0005
                l = min(o, c) * 0.9995
                v = 5000.0
                rows1h.append(OhlcvRow(symbol, "1h", ts, o, h, l, c, v))
                price = c

            store.upsert_ohlcv_rows(rows15 + rows1h)

            strat = TradingStrategy()
            cfg = ReplayConfig(warmup_bars=20, max_open_positions=1, cooldown_seconds=0, risk_per_trade=0.005)
            eng = ReplayEngine(store, strat, cfg=cfg)

            res = eng.run([symbol], start=start, end=start + timedelta(hours=10), initial_equity=200.0)
            self.assertIn("trade_count", res.metrics)
            self.assertGreaterEqual(res.metrics["trade_count"], 0)
        finally:
            try:
                os.unlink(db_path)
            except Exception:
                pass

    def test_replay_payload_contains_execution_audit_fields(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            store = ResearchStore(db_path)
            store.init_schema()

            symbol = "BTC/USDT"
            start = datetime(2025, 1, 1, tzinfo=UTC)

            rows15 = []
            price = 100.0
            for i in range(180):
                ts = int((start + timedelta(minutes=15 * i)).timestamp() * 1000)
                o = price
                c = price * 1.01
                h = c * 1.01
                l = o * 0.997
                v = 1000.0 + i
                rows15.append(OhlcvRow(symbol, "15m", ts, o, h, l, c, v))
                price = c

            rows1h = []
            price = 100.0
            for i in range(80):
                ts = int((start + timedelta(hours=i)).timestamp() * 1000)
                o = price
                c = price * 1.015
                h = c * 1.01
                l = o * 0.995
                v = 5000.0 + i
                rows1h.append(OhlcvRow(symbol, "1h", ts, o, h, l, c, v))
                price = c

            store.upsert_ohlcv_rows(rows15 + rows1h)

            cfg = ReplayConfig(warmup_bars=40, max_open_positions=1, cooldown_seconds=0, risk_per_trade=0.005)
            strategy = _AlwaysBuyStrategy()
            modeled = ReplayEngine(
                store,
                strategy,
                cfg=cfg,
                execution_model=DefaultExecutionModel(
                    taker_fee_bps=10.0,
                    base_slippage_bps=2.0,
                    slippage_atr_k=1.0,
                    max_slippage_bps=15.0,
                ),
            )
            legacy = ReplayEngine(
                store,
                strategy,
                cfg=cfg,
                execution_model=None,
                enable_execution_model=False,
            )

            end = start + timedelta(hours=36)
            modeled_res = modeled.run([symbol], start=start, end=end, initial_equity=200.0)
            legacy_res = legacy.run([symbol], start=start, end=end, initial_equity=200.0)

            self.assertGreaterEqual(int(modeled_res.metrics.get("trade_count") or 0), 1)
            self.assertTrue(modeled_res.trades)

            payload = _to_payload(modeled_res)
            self.assertTrue(payload["trades"])
            row = payload["trades"][0]
            self.assertIn("entry_slippage_bps", row)
            self.assertIn("exit_slippage_bps", row)
            self.assertIn("entry_fee_bps", row)
            self.assertIn("exit_fee_bps", row)
            self.assertIn("execution_note", row)
            self.assertIsNotNone(row["entry_slippage_bps"])
            self.assertIsNotNone(row["exit_slippage_bps"])
            self.assertIsNotNone(row["entry_fee_bps"])
            self.assertIsNotNone(row["exit_fee_bps"])
            self.assertIsNotNone(row["execution_note"])

            first_trade = asdict(modeled_res.trades[0])
            self.assertIn("entry_slippage_bps", first_trade)
            self.assertIn("exit_slippage_bps", first_trade)

            legacy_payload = _to_payload(legacy_res)
            self.assertTrue(legacy_payload["trades"])
            legacy_row = legacy_payload["trades"][0]
            self.assertIn("entry_slippage_bps", legacy_row)
            self.assertIn("exit_slippage_bps", legacy_row)
            self.assertIn("entry_fee_bps", legacy_row)
            self.assertIn("exit_fee_bps", legacy_row)
            self.assertIn("execution_note", legacy_row)
            self.assertIsNone(legacy_row["entry_slippage_bps"])
            self.assertIsNone(legacy_row["exit_slippage_bps"])
            self.assertIsNone(legacy_row["entry_fee_bps"])
            self.assertIsNone(legacy_row["exit_fee_bps"])

            self.assertNotEqual(float(modeled_res.final_equity), float(legacy_res.final_equity))
        finally:
            try:
                os.unlink(db_path)
            except Exception:
                pass

    def test_replay_with_portfolio_risk_enabled_reports_metrics(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            store = ResearchStore(db_path)
            store.init_schema()
            symbol = "BTC/USDT"
            start = datetime(2025, 1, 1, tzinfo=UTC)

            rows15 = []
            price = 100.0
            for i in range(120):
                ts = int((start + timedelta(minutes=15 * i)).timestamp() * 1000)
                o = price
                c = price * 1.005
                h = c * 1.003
                l = o * 0.998
                rows15.append(OhlcvRow(symbol, "15m", ts, o, h, l, c, 1000.0 + i))
                price = c

            rows1h = []
            price = 100.0
            for i in range(60):
                ts = int((start + timedelta(hours=i)).timestamp() * 1000)
                o = price
                c = price * 1.01
                h = c * 1.003
                l = o * 0.997
                rows1h.append(OhlcvRow(symbol, "1h", ts, o, h, l, c, 4000.0 + i))
                price = c
            store.upsert_ohlcv_rows(rows15 + rows1h)

            cfg = ReplayConfig(warmup_bars=30, max_open_positions=2, cooldown_seconds=0, risk_per_trade=0.005)
            strategy = _AlwaysBuyStrategy()
            pr_engine = PortfolioRiskEngine(
                RiskLimits(
                    max_gross_exposure_pct=1.5,
                    max_net_exposure_pct=1.0,
                    max_per_symbol_exposure_pct=0.6,
                    max_concurrent_positions=0,
                )
            )
            eng = ReplayEngine(
                store,
                strategy,
                cfg=cfg,
                portfolio_risk_engine=pr_engine,
                enable_portfolio_risk=True,
            )
            res = eng.run([symbol], start=start, end=start + timedelta(hours=24), initial_equity=200.0)
            self.assertIn("portfolio_risk_blocks", res.metrics)
            self.assertIn("portfolio_risk_scaled", res.metrics)
            self.assertGreaterEqual(int(res.metrics["portfolio_risk_blocks"]), 1)
        finally:
            try:
                os.unlink(db_path)
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()
