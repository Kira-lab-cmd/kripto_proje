import unittest

from backend.edge_stability_engine import evaluate_edge_stability


def _trades() -> list[dict]:
    out = []
    for i in range(40):
        pnl = 2.0 if i % 3 else -1.5
        out.append(
            {
                "symbol": "BTC/USDT",
                "entry_ts_ms": 1_700_000_000_000 + i * 60_000,
                "realized_pnl": pnl,
                "r_multiple": pnl / 2.0,
            }
        )
    return out


class EdgeStabilitySmokeTests(unittest.TestCase):
    def test_stability_keys_exist(self):
        res = evaluate_edge_stability({"trades": _trades()})
        self.assertIn("trade_count", res)
        self.assertIn("net_pnl", res)
        self.assertIn("win_rate", res)
        self.assertIn("max_dd_proxy", res)
        self.assertIn("objective", res)
        self.assertIn("stability_score", res)
        self.assertIn("tail_pnl", res)
        self.assertIn("notes", res)

    def test_deterministic_output(self):
        p = {"trades": _trades()}
        r1 = evaluate_edge_stability(p)
        r2 = evaluate_edge_stability(p)
        self.assertEqual(r1, r2)


if __name__ == "__main__":
    unittest.main()
