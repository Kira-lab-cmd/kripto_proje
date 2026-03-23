import json
import os
import tempfile
import unittest

from backend.edge_diagnostics import PolicyRecipe, Rule
from backend.overlay_policy import DataDrivenOverlayPolicy, OverlayContext, build_overlay_policy, load_policy_recipe


def _ctx(**kwargs):
    base = {
        "symbol": "BTC/USDT",
        "ts_ms": 1730000000000,
        "side": "BUY",
        "score": 3.0,
        "buy_th": 2.0,
        "sell_th": -2.0,
        "atr_pct": 0.002,
        "adx": 20.0,
        "er": 0.3,
        "regime": "CHOP",
        "trend_dir_1h": "UP",
    }
    base.update(kwargs)
    return OverlayContext(**base)


class DataDrivenOverlayPolicyTests(unittest.TestCase):
    def _write_policy(self, payload: dict) -> str:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
            json.dump(payload, f)
            return f.name

    def test_missing_file_falls_back_to_noop(self):
        policy = DataDrivenOverlayPolicy("backend/config/this_file_should_not_exist.json")
        d = policy.decide(_ctx())
        self.assertEqual(d.risk_scalar, 1.0)
        self.assertFalse(d.block_buy)
        self.assertFalse(d.block_sell)
        self.assertEqual(d.buy_th_add, 0.0)
        self.assertEqual(d.sell_th_add, 0.0)
        self.assertIsNone(d.note)

    def test_invalid_json_falls_back_to_noop(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
            f.write("{not-json")
            path = f.name
        try:
            policy = DataDrivenOverlayPolicy(path)
            d = policy.decide(_ctx())
            self.assertEqual(d.risk_scalar, 1.0)
            self.assertIsNone(d.note)
        finally:
            try:
                os.unlink(path)
            except Exception:
                pass

    def test_rule_effects_are_combined_in_order(self):
        recipe = PolicyRecipe(
            rules=[
                Rule(
                    name="r1",
                    predicate={"regime": ["CHOP"]},
                    effect={"risk_scalar": 0.8, "buy_th_add": 0.1},
                    note=None,
                ),
                Rule(
                    name="r2",
                    predicate={"side": ["BUY"], "atr_pct": {"lt": 0.003}},
                    effect={"risk_scalar": 0.5, "sell_th_add": 0.2, "block_buy": True},
                    note=None,
                ),
            ],
            meta={},
        )
        policy = DataDrivenOverlayPolicy(recipe=recipe)
        d = policy.decide(_ctx())
        self.assertAlmostEqual(d.risk_scalar, 0.4, places=9)
        self.assertTrue(d.block_buy)
        self.assertFalse(d.block_sell)
        self.assertAlmostEqual(d.buy_th_add, 0.1, places=9)
        self.assertAlmostEqual(d.sell_th_add, 0.2, places=9)
        self.assertIn("r1", d.note or "")
        self.assertIn("r2", d.note or "")

    def test_recipe_path_load_and_builder(self):
        payload = {
            "version": 1,
            "rules": [
                {
                    "name": "dd_001",
                    "when": {"regime": ["CHOP"], "side": ["BUY"]},
                    "then": {"risk_scalar": 0.7, "buy_th_add": 0.2},
                    "note": "sample",
                }
            ],
            "meta": {"source": "unit"},
        }
        path = self._write_policy(payload)
        try:
            recipe = load_policy_recipe(path)
            self.assertEqual(len(recipe.rules), 1)
            policy = build_overlay_policy("data_driven", recipe_path=path)
            d = policy.decide(_ctx())
            self.assertAlmostEqual(d.risk_scalar, 0.7, places=9)
            self.assertAlmostEqual(d.buy_th_add, 0.2, places=9)
        finally:
            try:
                os.unlink(path)
            except Exception:
                pass

    def test_no_match_returns_noop(self):
        payload = {
            "version": 1,
            "rules": [
                {"name": "x", "when": {"side": ["SELL"]}, "then": {"risk_scalar": 0.1}},
            ],
        }
        path = self._write_policy(payload)
        try:
            policy = DataDrivenOverlayPolicy(path)
            d = policy.decide(_ctx(side="BUY"))
            self.assertEqual(d.risk_scalar, 1.0)
            self.assertEqual(d.buy_th_add, 0.0)
            self.assertEqual(d.sell_th_add, 0.0)
            self.assertFalse(d.block_buy)
            self.assertFalse(d.block_sell)
            self.assertIsNone(d.note)
        finally:
            try:
                os.unlink(path)
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()
