# File: backend/tests/test_adaptive_threshold.py
"""Unittest version of adaptive threshold tests.

We intentionally avoid pytest dependency because the repo runs with
`python -m unittest` in production CI.
"""

from __future__ import annotations

import unittest

from backend.strategies.adaptive_threshold import AdaptiveThresholdConfig, adjust_threshold_for_1h


class AdaptiveThresholdTests(unittest.TestCase):
    def test_adjust_threshold_matrix(self) -> None:
        cfg = AdaptiveThresholdConfig(
            base_threshold=60,
            aligned_delta=-5,
            neutral_delta=0,
            opposed_delta=10,
        )

        cases = [
            ("LONG", "UP", 60, 55),
            ("LONG", "NEUTRAL", 60, 60),
            ("LONG", "DOWN", 60, 70),
            ("SHORT", "DOWN", 60, 55),
            ("SHORT", "NEUTRAL", 60, 60),
            ("SHORT", "UP", 60, 70),
            ("LONG", "UNKNOWN", 60, 60),
        ]

        for side, dir_1h, base, expected in cases:
            out = adjust_threshold_for_1h(base_threshold=base, trade_side=side, dir_1h=dir_1h, cfg=cfg)
            self.assertEqual(out, expected)


if __name__ == "__main__":
    unittest.main()
