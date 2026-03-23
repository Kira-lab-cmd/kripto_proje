from datetime import datetime, timedelta

from backend.config import settings


class CorrelationGuard:
    def __init__(self) -> None:
        self.freeze_until: dict[str, datetime] = {}

    def is_frozen(self, symbol: str, now: datetime) -> bool:
        until = self.freeze_until.get(symbol)
        return bool(until and until > now)

    def evaluate(self, symbol: str, corr_to_open_cluster: float, now: datetime) -> tuple[bool, str | None]:
        if corr_to_open_cluster > settings.corr_kill_threshold:
            self.freeze_until[symbol] = now + timedelta(minutes=settings.corr_freeze_minutes)
            return False, f"corr_block({symbol} corr={corr_to_open_cluster:.2f})"
        if self.is_frozen(symbol, now):
            return False, "corr_freeze_active"
        return True, None
