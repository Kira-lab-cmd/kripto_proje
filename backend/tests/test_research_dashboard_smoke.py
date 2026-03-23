import unittest

from backend.research_dashboard import build_research_dashboard


def _replay_payload() -> dict:
    return {
        "initial_equity": 200.0,
        "final_equity": 215.0,
        "metrics": {"trade_count": 3, "net_pnl": 15.0, "win_rate": 0.66, "avg_r_multiple": 0.25},
        "trades": [
            {"symbol": "BTC/USDT", "entry_ts_ms": 1, "realized_pnl": 4.0, "r_multiple": 0.5},
            {"symbol": "ETH/USDT", "entry_ts_ms": 2, "realized_pnl": -2.0, "r_multiple": -0.3},
            {"symbol": "BTC/USDT", "entry_ts_ms": 3, "realized_pnl": 13.0, "r_multiple": 0.7},
        ],
    }


def _walkforward_payload() -> dict:
    return {
        "aggregated": {
            "fold_count": 2,
            "total_test_net_pnl": 10.0,
            "avg_test_win_rate": 0.6,
            "avg_test_trade_count": 4.0,
            "avg_test_max_dd_pct": 0.1,
        },
        "equity_curve_summary": {"initial_equity": 200.0, "final_equity": 210.0},
    }


class ResearchDashboardSmokeTests(unittest.TestCase):
    def test_dashboard_contains_sections(self):
        out = build_research_dashboard(
            replay_payload=_replay_payload(),
            walkforward_payload=_walkforward_payload(),
            include_edge_stability=True,
        )
        self.assertIn("replay", out)
        self.assertIn("walkforward", out)
        self.assertIn("edge_stability", out)
        self.assertIn("highlights", out)
        self.assertTrue(out["replay"]["available"])
        self.assertTrue(out["walkforward"]["available"])


if __name__ == "__main__":
    unittest.main()
