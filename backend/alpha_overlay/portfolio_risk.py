from __future__ import annotations


class PortfolioRisk:
    @staticmethod
    def adjust_risk_budget(
        base_risk: float,
        equity: float,
        dd_pct: float | None,
        regime: str | None,
        atr_pct: float | None,
        corr_exposure: float | None,
    ) -> float:
        del equity
        del corr_exposure
        risk = float(base_risk)

        if dd_pct is not None and dd_pct > 0.10:
            if dd_pct >= 0.20:
                risk *= 0.50
            else:
                frac = (float(dd_pct) - 0.10) / 0.10
                risk *= 1.0 - (0.50 * max(0.0, min(1.0, frac)))

        if atr_pct is not None and float(atr_pct) < 0.002:
            risk *= 0.70

        if str(regime or "").upper() == "CHOP":
            risk *= 0.60

        return max(0.0, risk)
