import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


class AnalyzeWalkforwardTests(unittest.TestCase):
    def test_analyze_walkforward_generates_reports(self):
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            in_path = td_path / "walkforward.json"
            payload = {
                "folds": [
                    {
                        "fold": 0,
                        "test_start_ms": 1735689600000,
                        "test_end_ms": 1736294400000,
                        "best_params": {"buy_th": 2.5, "sell_th": -2.5, "atr_sl_mult": 2, "atr_tp_mult": 3},
                        "test_metrics": {"net_pnl": 10.0, "win_rate": 0.6, "trade_count": 8},
                        "test_max_dd_pct": 0.12,
                    },
                    {
                        "fold": 1,
                        "test_start_ms": 1736294400000,
                        "test_end_ms": 1736899200000,
                        "best_params": {"buy_th": 2.5, "sell_th": -2.5, "atr_sl_mult": 2, "atr_tp_mult": 3},
                        "test_metrics": {"net_pnl": -4.0, "win_rate": 0.4, "trade_count": 6},
                        "test_max_dd_pct": 0.20,
                    },
                ],
                "equity_curve_summary": {"initial_equity": 200.0, "final_equity": 206.0},
            }
            in_path.write_text(json.dumps(payload), encoding="utf-8")

            proc = subprocess.run(
                [sys.executable, "-m", "backend.analyze_walkforward", "--in", str(in_path)],
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)

            report_json = td_path / "report.json"
            report_md = td_path / "report.md"
            self.assertTrue(report_json.exists())
            self.assertTrue(report_md.exists())

            report = json.loads(report_json.read_text(encoding="utf-8"))
            self.assertIn("summary", report)
            self.assertIn("stability", report)
            self.assertIn("param_frequency", report)


if __name__ == "__main__":
    unittest.main()
