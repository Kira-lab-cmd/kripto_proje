from datetime import UTC, datetime
import unittest

from backend.config import DB_PATH, RESEARCH_DB_PATH, settings
from backend.domain.models import Regime, Sleeve
from backend.risk.correlation_guard import CorrelationGuard
from backend.risk.execution_health import ExecutionHealthScaler
from backend.risk.portfolio_heat import PortfolioHeatManager
from backend.strategy import GATE_STATUS_KEYS, TradingStrategy
from backend.strategy.long_trend_hold import LongTrendHoldStrategy
from backend.strategy.medium_continuation import MediumContinuationStrategy
from backend.strategy.regime_guard import RegimeGuard
from backend.strategy.router import UNIVERSE_TO_SLEEVE
from backend.strategy.short_snapback import ShortSnapbackStrategy


class StrategyPackageCompatTests(unittest.TestCase):
    def test_legacy_strategy_reexport_survives_package_split(self):
        self.assertTrue(callable(TradingStrategy))
        self.assertIn("breakout_ok", GATE_STATUS_KEYS)

    def test_lowercase_settings_aliases_work_for_new_modules(self):
        self.assertEqual(settings.regime_allowed_short, settings.REGIME_ALLOWED_SHORT)
        self.assertEqual(settings.short_universe, settings.SHORT_UNIVERSE)
        self.assertEqual(DB_PATH.name, "trading_bot.db")
        self.assertEqual(RESEARCH_DB_PATH.name, "research.db")

    def test_router_and_guard_modules_load(self):
        self.assertEqual(UNIVERSE_TO_SLEEVE["BTC/USDT"], Sleeve.LONG)
        guard = RegimeGuard()
        regime = guard.detect({"dir_1h": "UP", "adx": 25.0, "er": 0.55, "atr_pct": 0.01})
        self.assertEqual(regime, Regime.BULLISH)

    def test_new_strategy_models_return_signal_data(self):
        short_signal = ShortSnapbackStrategy().get_signal(
            symbol="SUI/USDT",
            price=1.25,
            features={
                "atr_pct": 0.005,
                "pullback_score": 0.2,
                "reclaim_score": 0.05,
                "below_ema_fast": True,
                "dir_1h": "UP",
                "regime": "BULLISH",
            },
        )
        medium_signal = MediumContinuationStrategy().get_signal(
            symbol="LINK/USDT",
            price=20.0,
            features={
                "atr_pct": 0.01,
                "dir_1h": "UP",
                "vol_ratio": 1.2,
                "breakout_ok": True,
                "retest_ok": True,
                "regime": "BULLISH",
            },
        )
        long_signal = LongTrendHoldStrategy().get_signal(
            symbol="BTC/USDT",
            price=68000.0,
            features={
                "atr_pct": 0.01,
                "dir_1h": "UP",
                "dir_4h": "UP",
                "ema200_up": True,
                "weekly_structure_ok": True,
                "regime": "BULLISH",
            },
        )
        self.assertEqual(short_signal.sleeve, Sleeve.SHORT)
        self.assertEqual(medium_signal.sleeve, Sleeve.MEDIUM)
        self.assertEqual(long_signal.sleeve, Sleeve.LONG)

    def test_risk_helpers_are_constructible(self):
        heat_mgr = PortfolioHeatManager()
        corr_guard = CorrelationGuard()
        exec_scaler = ExecutionHealthScaler()
        allowed, reason = heat_mgr.can_allocate(Sleeve.SHORT, 0.001, 0.001, 0.001)
        self.assertTrue(allowed)
        self.assertIsNone(reason)
        self.assertFalse(corr_guard.is_frozen("BTC/USDT", datetime.now(tz=UTC)))
        self.assertGreater(exec_scaler.scale(Sleeve.LONG, 4.0, 1, False), 0.0)


if __name__ == "__main__":
    unittest.main()
