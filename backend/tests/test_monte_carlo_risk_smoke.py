import json
import subprocess
import sys
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path

from backend.monte_carlo_risk import MCRiskConfig, run_monte_carlo

REPO_ROOT = Path(__file__).resolve().parents[2]


def _synthetic_trades() -> list[dict]:
    trades: list[dict] = []
    for i in range(60):
        pnl = 2.0 if i % 3 != 0 else -3.0
        trades.append(
            {
                "symbol": "BTC/USDT",
                "side": "BUY",
                "entry_ts_ms": 1_700_000_000_000 + i * 60_000,
                "exit_ts_ms": 1_700_000_000_000 + (i + 1) * 60_000,
                "realized_pnl": pnl,
                "r_multiple": pnl / 2.0,
                "fee_paid": 0.1,
            }
        )
    return trades


class MonteCarloRiskSmokeTests(unittest.TestCase):
    def test_run_monte_carlo_smoke_and_keys(self):
        cfg = MCRiskConfig(n_sims=200, block_size=2, seed=42)
        res = run_monte_carlo(_synthetic_trades(), initial_equity=200.0, config=cfg, mode="pnl")
        payload = asdict(res)
        self.assertIn("prob_ruin", payload)
        self.assertIn("max_dd_stats", payload)
        self.assertIn("final_equity_stats", payload)
        self.assertIn("worst_10_avg_final_equity", payload)
        self.assertIn("dd_series_summary", payload)
        self.assertIn("notes", payload)
        self.assertIn("p95", payload["max_dd_stats"])
        self.assertIn("p99", payload["max_dd_stats"])

    def test_deterministic_given_same_seed(self):
        cfg = MCRiskConfig(n_sims=300, block_size=3, seed=7)
        r1 = asdict(run_monte_carlo(_synthetic_trades(), 200.0, cfg, mode="pnl"))
        r2 = asdict(run_monte_carlo(_synthetic_trades(), 200.0, cfg, mode="pnl"))
        self.assertEqual(r1, r2)

    def test_no_trades_raises(self):
        cfg = MCRiskConfig(n_sims=10, seed=1)
        with self.assertRaisesRegex(ValueError, "no_trades"):
            run_monte_carlo([], 200.0, cfg, mode="pnl")

    def test_cli_smoke(self):
        payload = {
            "initial_equity": 200.0,
            "trades": _synthetic_trades(),
            "metrics": {},
        }
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w", encoding="utf-8") as f:
            in_path = f.name
            json.dump(payload, f)
        try:
            proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "backend.monte_carlo_cli",
                    "--input",
                    in_path,
                    "--n-sims",
                    "200",
                    "--seed",
                    "7",
                ],
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            out = json.loads(proc.stdout)
            self.assertIn("prob_ruin", out)
            self.assertIn("max_dd_stats", out)
        finally:
            Path(in_path).unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
