import unittest

from backend.execution_model import DefaultExecutionModel, ExecutionContext


class ExecutionModelTests(unittest.TestCase):
    def test_buy_increases_price_sell_decreases(self):
        model = DefaultExecutionModel(
            taker_fee_bps=10.0,
            base_slippage_bps=2.0,
            slippage_atr_k=1.0,
            max_slippage_bps=15.0,
        )
        buy_fill = model.fill(
            ExecutionContext(
                symbol="BTC/USDT",
                ts_ms=0,
                action="BUY",
                qty=1.0,
                ref_price=100.0,
                atr_pct=0.01,
                is_entry=True,
                side_position="BUY",
                regime="TREND",
                vol_ratio=1.0,
                trend_dir_1h="UP",
            )
        )
        sell_fill = model.fill(
            ExecutionContext(
                symbol="BTC/USDT",
                ts_ms=0,
                action="SELL",
                qty=1.0,
                ref_price=100.0,
                atr_pct=0.01,
                is_entry=False,
                side_position="BUY",
                regime="TREND",
                vol_ratio=1.0,
                trend_dir_1h="UP",
            )
        )

        self.assertGreater(buy_fill.exec_price, 100.0)
        self.assertLess(sell_fill.exec_price, 100.0)
        self.assertGreaterEqual(buy_fill.slippage_bps, 0.0)
        self.assertGreaterEqual(sell_fill.slippage_bps, 0.0)

    def test_fee_scales_with_notional_and_bps(self):
        model = DefaultExecutionModel(
            taker_fee_bps=12.5,
            base_slippage_bps=0.0,
            slippage_atr_k=0.0,
            max_slippage_bps=None,
        )
        fill = model.fill(
            ExecutionContext(
                symbol="ETH/USDT",
                ts_ms=0,
                action="BUY",
                qty=2.0,
                ref_price=100.0,
                atr_pct=None,
                is_entry=True,
                side_position="BUY",
                regime=None,
                vol_ratio=None,
                trend_dir_1h=None,
            )
        )

        expected_notional = 200.0
        expected_fee = expected_notional * (12.5 / 10_000.0)
        self.assertAlmostEqual(fill.exec_price, 100.0, places=9)
        self.assertAlmostEqual(fill.fee_paid, expected_fee, places=9)
        self.assertAlmostEqual(fill.fee_bps, 12.5, places=9)

    def test_slippage_clamp_applies(self):
        model = DefaultExecutionModel(
            taker_fee_bps=10.0,
            base_slippage_bps=2.0,
            slippage_atr_k=5.0,
            max_slippage_bps=15.0,
        )
        fill = model.fill(
            ExecutionContext(
                symbol="BTC/USDT",
                ts_ms=0,
                action="BUY",
                qty=1.0,
                ref_price=100.0,
                atr_pct=0.02,
                is_entry=True,
            )
        )
        self.assertAlmostEqual(fill.slippage_bps, 15.0, places=9)

    def test_fill_is_deterministic_for_same_context(self):
        model = DefaultExecutionModel(
            taker_fee_bps=10.0,
            base_slippage_bps=2.0,
            slippage_atr_k=1.0,
            max_slippage_bps=15.0,
        )
        ctx = ExecutionContext(
            symbol="BTC/USDT",
            ts_ms=123,
            action="BUY",
            qty=0.1234,
            ref_price=100.5,
            atr_pct=0.004,
            is_entry=True,
            side_position="BUY",
        )
        f1 = model.fill(ctx)
        f2 = model.fill(ctx)
        self.assertEqual(f1, f2)

    def test_rounding_and_min_notional_rejection(self):
        model = DefaultExecutionModel(
            taker_fee_bps=10.0,
            base_slippage_bps=0.0,
            slippage_atr_k=0.0,
            max_slippage_bps=None,
            min_notional=10.0,
            step_size=0.01,
            tick_size=0.1,
        )
        rejected = model.fill(
            ExecutionContext(
                symbol="BTC/USDT",
                ts_ms=0,
                action="BUY",
                qty=0.123,
                ref_price=80.0,
                atr_pct=0.0,
                is_entry=True,
            )
        )
        self.assertTrue(rejected.rejected)
        self.assertEqual(rejected.fee_paid, 0.0)
        self.assertEqual(rejected.filled_qty, 0.0)

        accepted = model.fill(
            ExecutionContext(
                symbol="BTC/USDT",
                ts_ms=0,
                action="BUY",
                qty=0.127,
                ref_price=100.04,
                atr_pct=0.0,
                is_entry=True,
            )
        )
        self.assertFalse(accepted.rejected)
        self.assertAlmostEqual(accepted.filled_qty or 0.0, 0.12, places=9)
        self.assertAlmostEqual(accepted.exec_price, 100.0, places=9)


if __name__ == "__main__":
    unittest.main()
