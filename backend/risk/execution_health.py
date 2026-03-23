from backend.domain.models import Sleeve


class ExecutionHealthScaler:
    def scale(self, sleeve: Sleeve, spread_bps: float, stale_seconds: int, has_recent_429: bool) -> float:
        mult = 1.0
        if spread_bps > 8:
            mult *= 0.75
        if spread_bps > 12:
            mult *= 0.50
        if stale_seconds > 8:
            mult *= 0.70
        if has_recent_429:
            mult *= 0.40
        return max(mult, 0.0)
