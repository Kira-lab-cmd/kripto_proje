import json
import os
import tempfile
import unittest
from pathlib import Path

from backend.policy_generator import generate_data_driven_policy, load_policy_recipe, write_policy_recipe


def _trade(i: int, pnl: float, regime: str = "CHOP") -> dict:
    side = "BUY" if i % 2 == 0 else "SELL"
    return {
        "symbol": "BTC/USDT",
        "side": side,
        "entry_ts_ms": 1_700_000_000_000 + i * 60_000,
        "score": 3.0 if side == "BUY" else -3.0,
        "buy_th": 2.0,
        "sell_th": -2.0,
        "r_multiple": pnl / 3.0,
        "realized_pnl": pnl,
        "fee_paid": 0.1,
        "regime": regime,
        "adx": 20.0,
        "er": 0.3,
        "atr_pct": 0.002 if regime == "CHOP" else 0.007,
        "trend_dir_1h": "UP" if side == "BUY" else "DOWN",
        "entry_slippage_bps": 3.0,
        "exit_slippage_bps": 4.0,
        "reason": "unit",
    }


class PolicyGeneratorTests(unittest.TestCase):
    def test_generate_and_write_recipe(self):
        trades = []
        for i in range(100):
            if i < 60:
                trades.append(_trade(i, pnl=-1.0, regime="CHOP"))
            else:
                trades.append(_trade(i, pnl=1.0, regime="TREND"))
        policy, recipe = generate_data_driven_policy(
            trades,
            constraints={"min_samples_per_rule": 20, "objective_lambda": 100.0, "top_k": 5},
        )
        self.assertIsNotNone(policy)
        self.assertIsNotNone(recipe)
        self.assertTrue(hasattr(policy, "decide"))
        self.assertIsInstance(recipe.rules, list)

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            out_path = f.name
        try:
            write_policy_recipe(out_path, recipe)
            loaded = load_policy_recipe(out_path)
            self.assertIsInstance(loaded.rules, list)
            raw = json.loads(Path(out_path).read_text(encoding="utf-8"))
            self.assertIn("rules", raw)
            self.assertIn("meta", raw)
        finally:
            try:
                os.unlink(out_path)
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()
