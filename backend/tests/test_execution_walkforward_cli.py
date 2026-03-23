import os
import subprocess
import sys
import tempfile
import unittest
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend.execution_model import ExecutionEngine
from backend.replay_engine import ReplayConfig, ReplayEngine
from backend.research_store import OhlcvRow, ResearchStore

UTC = timezone.utc
REPO_ROOT = Path(__file__).resolve().parents[2]


class _AlwaysBuyStrategy:
    def get_signal(self, ohlcv, sentiment_score, symbol, trend_dir_1h=None):
        px = float(ohlcv[-1][4])
        return {
            "signal": "BUY",
            "score": 3.5,
            "current_price": px,
            "stop_loss": px * 0.99,
            "take_profit": px * 1.02,
            "reason": "unit_test_stub",
            "regime": "TREND",
            "regime_conf": 0.8,
            "adx": 28.0,
            "er": 0.4,
            "atr_pct": 0.006,
            "vol_ratio": 1.1,
            "dir_1h": trend_dir_1h or "UNKNOWN",
            "is_uptrend": True,
            "ema200": px * 0.95,
            "atr": px * 0.006,
            "effective_thresholds": {"buy": 2.0, "sell": -2.0},
        }


def _build_synthetic_store(db_path: str) -> tuple[ResearchStore, datetime]:
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
    return store, start


def _trade_snapshot(trades):
    out = []
    for t in trades:
        row = asdict(t)
        exec_meta = getattr(t, "execution_meta", None)
        if isinstance(exec_meta, dict):
            row["execution_meta"] = exec_meta
        out.append(row)
    return out


class ExecutionWalkforwardCliTests(unittest.TestCase):
    def test_smoke_imports_new_modules(self):
        import backend.execution_model as em
        import backend.replay as replay_mod
        import backend.walkforward as wf_mod

        self.assertTrue(hasattr(em, "ExecutionEngine"))
        self.assertTrue(hasattr(em, "DefaultExecutionModel"))
        self.assertTrue(hasattr(replay_mod, "_build_execution_engine"))
        self.assertTrue(hasattr(wf_mod, "main"))

    def test_replay_execution_is_deterministic_for_same_seed(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            store, start = _build_synthetic_store(db_path)
            cfg = ReplayConfig(warmup_bars=40, max_open_positions=1, cooldown_seconds=0, risk_per_trade=0.005)

            strategy = _AlwaysBuyStrategy()
            eng1 = ReplayEngine(
                store,
                strategy,
                cfg=cfg,
                execution_engine=ExecutionEngine.from_realistic(
                    fee_bps_maker=10.0,
                    fee_bps_taker=10.0,
                    slippage_bps=2.0,
                    seed=42,
                ),
            )
            eng2 = ReplayEngine(
                store,
                strategy,
                cfg=cfg,
                execution_engine=ExecutionEngine.from_realistic(
                    fee_bps_maker=10.0,
                    fee_bps_taker=10.0,
                    slippage_bps=2.0,
                    seed=42,
                ),
            )

            end = start + timedelta(hours=36)
            res1 = eng1.run(["BTC/USDT"], start=start, end=end, initial_equity=200.0, sentiment_score=0.0)
            res2 = eng2.run(["BTC/USDT"], start=start, end=end, initial_equity=200.0, sentiment_score=0.0)

            self.assertEqual(res1.metrics, res2.metrics)
            self.assertEqual(_trade_snapshot(res1.trades), _trade_snapshot(res2.trades))
            self.assertGreaterEqual(int(res1.metrics.get("trade_count") or 0), 1)
        finally:
            try:
                os.unlink(db_path)
            except Exception:
                pass

    def test_replay_cli_help_contains_execution_flags(self):
        proc = subprocess.run(
            [sys.executable, "-m", "backend.replay", "--help"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        out = (proc.stdout or "") + (proc.stderr or "")
        self.assertIn("--taker-fee-bps", out)
        self.assertIn("--base-slippage-bps", out)
        self.assertIn("--slippage-atr-k", out)
        self.assertIn("--max-slippage-bps", out)
        self.assertIn("--no-execution-model", out)
        self.assertIn("--overlay", out)
        self.assertIn("data", out)
        self.assertIn("data_driven", out)
        self.assertIn("--overlay-recipe", out)
        self.assertIn("--overlay-policy-path", out)
        self.assertIn("--enable-overlay", out)
        self.assertIn("--enable-portfolio-risk", out)
        self.assertIn("--max-gross-exposure-pct", out)
        self.assertIn("--max-net-exposure-pct", out)
        self.assertIn("--max-per-symbol-exposure-pct", out)
        self.assertIn("--max-concurrent-positions", out)
        self.assertIn("--generate-policy", out)
        self.assertIn("--policy-out", out)
        self.assertIn("--mc-risk", out)
        self.assertIn("--mc-mode", out)
        self.assertIn("--mc-n-sims", out)
        self.assertIn("--mc-block-size", out)
        self.assertIn("--mc-ruin-floor-pct", out)
        self.assertIn("--mc-cost-mult", out)
        self.assertIn("--mc-pnl-shrink", out)
        self.assertIn("--mc-loss-tail-mult", out)
        self.assertIn("--mc-seed", out)
        self.assertIn("--edge-stability", out)
        self.assertIn("--dashboard", out)
        self.assertIn("--dump-policy", out)
        self.assertIn("--true-overlay", out)

    def test_walkforward_cli_help_contains_execution_flags(self):
        proc = subprocess.run(
            [sys.executable, "-m", "backend.walkforward", "--help"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        out = (proc.stdout or "") + (proc.stderr or "")
        self.assertIn("--execution-model", out)
        self.assertIn("--fee-bps-maker", out)
        self.assertIn("--fee-bps-taker", out)
        self.assertIn("--slippage-bps", out)
        self.assertIn("--seed", out)
        self.assertIn("--overlay", out)
        self.assertIn("data", out)
        self.assertIn("data_driven", out)
        self.assertIn("--overlay-recipe", out)
        self.assertIn("--overlay-config", out)
        self.assertIn("--overlay-policy-path", out)
        self.assertIn("--use-data-overlay", out)
        self.assertIn("--policy-in", out)
        self.assertIn("--enable-overlay", out)
        self.assertIn("--enable-portfolio-risk", out)
        self.assertIn("--max-gross-exposure-pct", out)
        self.assertIn("--max-net-exposure-pct", out)
        self.assertIn("--max-per-symbol-exposure-pct", out)
        self.assertIn("--max-concurrent-positions", out)
        self.assertIn("--generate-policy", out)
        self.assertIn("--policy-out", out)
        self.assertIn("--edge-stability", out)
        self.assertIn("--allocate-portfolio", out)
        self.assertIn("--dashboard", out)
        self.assertIn("--allocation", out)
        self.assertIn("--allocation-out", out)
        self.assertIn("--fail-on-leak", out)


if __name__ == "__main__":
    unittest.main()
