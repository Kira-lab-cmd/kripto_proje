import unittest

from backend.edge_diagnostics import (
    compute_segment_stats,
    extract_feature_rows,
    generate_rules,
    policy_recipe_to_dict,
)


def _trade(i: int, *, regime: str, side: str, pnl: float, atr_pct: float) -> dict:
    return {
        "symbol": "BTC/USDT" if i % 2 == 0 else "ETH/USDT",
        "side": side,
        "entry_ts_ms": 1_700_000_000_000 + i * 60_000,
        "score": 3.0 if side == "BUY" else -3.0,
        "buy_th": 2.0,
        "sell_th": -2.0,
        "r_multiple": pnl / 5.0,
        "realized_pnl": pnl,
        "fee_paid": 0.1,
        "regime": regime,
        "adx": 25.0,
        "er": 0.35,
        "atr_pct": atr_pct,
        "trend_dir_1h": "UP" if side == "BUY" else "DOWN",
        "entry_slippage_bps": 2.0,
        "exit_slippage_bps": 3.0,
        "reason": "unit",
    }


class EdgeDiagnosticsTests(unittest.TestCase):
    def test_generate_rules_respects_min_samples(self):
        trades = [_trade(i, regime="CHOP", side="BUY", pnl=-1.0, atr_pct=0.002) for i in range(20)]
        rows = extract_feature_rows(trades)
        stats = compute_segment_stats(rows, objective_lambda=100.0)
        recipe = generate_rules(
            stats,
            constraints={
                "min_samples_per_rule": 30,
                "objective_lambda": 100.0,
                "top_k": 8,
                "validation_stats": compute_segment_stats(rows, objective_lambda=100.0),
                "validation_rows": rows,
                "rows_all": rows,
            },
        )
        self.assertEqual(len(recipe.rules), 0)

    def test_generate_rules_is_deterministic_for_same_input(self):
        trades = []
        for i in range(120):
            if i % 2 == 0:
                trades.append(_trade(i, regime="CHOP", side="BUY", pnl=-1.2, atr_pct=0.002))
            else:
                trades.append(_trade(i, regime="TREND", side="SELL", pnl=1.0, atr_pct=0.007))
        rows = extract_feature_rows(trades)
        split = int(len(rows) * 0.7)
        train_rows = rows[:split]
        val_rows = rows[split:]
        train_stats = compute_segment_stats(train_rows, objective_lambda=100.0)
        val_stats = compute_segment_stats(val_rows, objective_lambda=100.0)

        recipe1 = generate_rules(
            train_stats,
            constraints={
                "min_samples_per_rule": 30,
                "objective_lambda": 100.0,
                "top_k": 8,
                "validation_stats": val_stats,
                "validation_rows": val_rows,
                "rows_all": rows,
            },
        )
        recipe2 = generate_rules(
            train_stats,
            constraints={
                "min_samples_per_rule": 30,
                "objective_lambda": 100.0,
                "top_k": 8,
                "validation_stats": val_stats,
                "validation_rows": val_rows,
                "rows_all": rows,
            },
        )
        self.assertEqual(policy_recipe_to_dict(recipe1), policy_recipe_to_dict(recipe2))

    def test_extract_and_stats_pipeline_smoke(self):
        trades = [_trade(i, regime="TREND", side="BUY", pnl=1.0, atr_pct=0.006) for i in range(40)]
        rows = extract_feature_rows(trades)
        self.assertEqual(len(rows), 40)
        stats = compute_segment_stats(rows, objective_lambda=100.0)
        self.assertIn("regime=TREND", stats)
        self.assertGreaterEqual(stats["regime=TREND"].count, 40)


if __name__ == "__main__":
    unittest.main()
