import unittest

from backend.portfolio_allocation import allocate_portfolio


def _payload() -> dict:
    return {
        "symbols": ["BTC/USDT", "ETH/USDT"],
        "trades": [
            {"symbol": "BTC/USDT", "realized_pnl": 5.0, "r_multiple": 0.8},
            {"symbol": "BTC/USDT", "realized_pnl": 3.0, "r_multiple": 0.5},
            {"symbol": "ETH/USDT", "realized_pnl": -1.0, "r_multiple": -0.2},
            {"symbol": "ETH/USDT", "realized_pnl": 1.0, "r_multiple": 0.1},
        ],
    }


class PortfolioAllocationSmokeTests(unittest.TestCase):
    def test_allocation_has_weights_and_capital(self):
        out = allocate_portfolio(_payload(), total_equity=1000.0)
        self.assertIn("allocations", out)
        allocs = out["allocations"]
        self.assertTrue(allocs)
        weight_sum = sum(float(a.get("weight", 0.0)) for a in allocs)
        self.assertAlmostEqual(weight_sum, 1.0, places=6)
        for a in allocs:
            self.assertIn("symbol", a)
            self.assertIn("weight", a)
            self.assertIn("capital", a)

    def test_deterministic(self):
        p = _payload()
        a1 = allocate_portfolio(p, total_equity=500.0)
        a2 = allocate_portfolio(p, total_equity=500.0)
        self.assertEqual(a1, a2)


if __name__ == "__main__":
    unittest.main()
