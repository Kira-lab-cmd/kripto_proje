from backend.domain.models import Sleeve
from backend.config import settings


class PortfolioHeatManager:
    def sleeve_limit(self, sleeve: Sleeve) -> float:
        if sleeve == Sleeve.SHORT:
            return settings.sleeve_max_heat_short
        if sleeve == Sleeve.MEDIUM:
            return settings.sleeve_max_heat_medium
        return settings.sleeve_max_heat_long

    def can_allocate(
        self,
        sleeve: Sleeve,
        requested_risk_pct: float,
        current_total_heat_pct: float,
        current_sleeve_heat_pct: float,
    ) -> tuple[bool, str | None]:
        if current_total_heat_pct + requested_risk_pct > settings.total_max_heat_pct:
            return False, "total_heat_exceeded"
        if current_sleeve_heat_pct + requested_risk_pct > self.sleeve_limit(sleeve):
            return False, f"{sleeve.value}_heat_exceeded"
        return True, None
