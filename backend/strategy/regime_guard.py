from backend.config import settings
from backend.domain.models import Regime, Sleeve


class RegimeGuard:
    def detect(self, market_state: dict) -> Regime:
        adx = market_state.get("adx", 0.0) or 0.0
        er = market_state.get("er", 0.0) or 0.0
        atr_pct = market_state.get("atr_pct", 0.0) or 0.0
        dir_1h = str(market_state.get("dir_1h", "NEUTRAL")).upper()

        if atr_pct > 0.035:
            return Regime.PANIC
        if dir_1h == "DOWN" and adx > 22:
            return Regime.BEARISH
        if dir_1h == "UP" and er > 0.45:
            return Regime.BULLISH
        if dir_1h in {"UP", "NEUTRAL"} and er > 0.25:
            return Regime.NEUTRAL_UP
        return Regime.RANGE

    def is_allowed(self, sleeve: Sleeve, regime: Regime) -> bool:
        allowed_map = {
            Sleeve.SHORT: {s.strip() for s in settings.regime_allowed_short.split(",")},
            Sleeve.MEDIUM: {s.strip() for s in settings.regime_allowed_medium.split(",")},
            Sleeve.LONG: {s.strip() for s in settings.regime_allowed_long.split(",")},
        }
        return regime.value in allowed_map[sleeve]
