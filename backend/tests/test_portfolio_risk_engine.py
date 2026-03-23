import unittest
from dataclasses import dataclass

from backend.portfolio_risk import PortfolioRiskEngine, RiskLimits


@dataclass
class _Pos:
    symbol: str
    side: str
    entry_price: float
    qty: float


class PortfolioRiskEngineTests(unittest.TestCase):
    def test_blocks_when_max_positions_reached(self):
        engine = PortfolioRiskEngine(
            RiskLimits(
                max_gross_exposure_pct=2.0,
                max_net_exposure_pct=2.0,
                max_per_symbol_exposure_pct=1.0,
                max_concurrent_positions=1,
            )
        )
        positions = {"BTC/USDT": _Pos(symbol="BTC/USDT", side="BUY", entry_price=100.0, qty=0.5)}
        d = engine.evaluate_entry(
            symbol="ETH/USDT",
            side="BUY",
            qty_prelim=0.2,
            price=100.0,
            positions=positions,
            equity=200.0,
            ts_ms=0,
        )
        self.assertFalse(d.allow_entry)
        self.assertEqual(d.risk_scalar, 0.0)
        self.assertIn("max_concurrent_positions", d.note)

    def test_scales_when_near_gross_cap(self):
        engine = PortfolioRiskEngine(
            RiskLimits(
                max_gross_exposure_pct=1.0,
                max_net_exposure_pct=2.0,
                max_per_symbol_exposure_pct=2.0,
                max_concurrent_positions=5,
            )
        )
        positions = {"BTC/USDT": _Pos(symbol="BTC/USDT", side="BUY", entry_price=90.0, qty=1.0)}
        d = engine.evaluate_entry(
            symbol="ETH/USDT",
            side="BUY",
            qty_prelim=0.2,
            price=100.0,
            positions=positions,
            equity=100.0,
            ts_ms=0,
        )
        self.assertTrue(d.allow_entry)
        self.assertAlmostEqual(d.risk_scalar, 0.5, places=9)
        self.assertIn("gross_scale", d.note)

    def test_per_symbol_cap_enforced(self):
        engine = PortfolioRiskEngine(
            RiskLimits(
                max_gross_exposure_pct=2.0,
                max_net_exposure_pct=2.0,
                max_per_symbol_exposure_pct=0.5,
                max_concurrent_positions=5,
            )
        )
        positions = {"BTC/USDT": _Pos(symbol="BTC/USDT", side="BUY", entry_price=100.0, qty=0.5)}
        d = engine.evaluate_entry(
            symbol="BTC/USDT",
            side="BUY",
            qty_prelim=0.2,
            price=100.0,
            positions=positions,
            equity=100.0,
            ts_ms=0,
        )
        self.assertFalse(d.allow_entry)
        self.assertEqual(d.risk_scalar, 0.0)
        self.assertIn("per_symbol_cap", d.note)

    def test_deterministic_for_same_input(self):
        engine = PortfolioRiskEngine(RiskLimits())
        positions = {"BTC/USDT": _Pos(symbol="BTC/USDT", side="BUY", entry_price=100.0, qty=0.2)}
        d1 = engine.evaluate_entry(
            symbol="ETH/USDT",
            side="BUY",
            qty_prelim=0.1,
            price=100.0,
            positions=positions,
            equity=200.0,
            ts_ms=123,
        )
        d2 = engine.evaluate_entry(
            symbol="ETH/USDT",
            side="BUY",
            qty_prelim=0.1,
            price=100.0,
            positions=positions,
            equity=200.0,
            ts_ms=123,
        )
        self.assertEqual(d1, d2)


if __name__ == "__main__":
    unittest.main()
