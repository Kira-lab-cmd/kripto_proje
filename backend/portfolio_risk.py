from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PortfolioState:
    equity: float
    open_positions: dict[str, Any]
    gross_exposure_usdt: float
    net_exposure_usdt: float
    symbol_exposure_usdt: dict[str, float] = field(default_factory=dict)
    realized_pnl_today: float = 0.0
    max_drawdown_proxy: float = 0.0


@dataclass
class RiskLimits:
    max_gross_exposure_pct: float = 1.50
    max_net_exposure_pct: float = 1.00
    max_per_symbol_exposure_pct: float = 0.60
    max_concurrent_positions: int = 2
    daily_loss_limit_pct: float | None = None
    correlation_guard: bool = False
    correlation_threshold: float = 0.80
    correlation_risk_scalar: float = 0.50
    correlation_block_threshold: float | None = None


@dataclass
class PortfolioRiskDecision:
    allow_entry: bool
    risk_scalar: float = 1.0
    note: str = ""


class PortfolioRiskEngine:
    def __init__(self, limits: RiskLimits | None = None):
        self.limits = limits or RiskLimits()

    @staticmethod
    def _position_notional(pos: Any) -> float:
        price = float(getattr(pos, "entry_price", 0.0) or 0.0)
        qty = float(getattr(pos, "qty", 0.0) or 0.0)
        return abs(price * qty)

    @staticmethod
    def _position_signed_notional(pos: Any) -> float:
        side = str(getattr(pos, "side", "") or "").upper()
        sign = 1.0 if side == "BUY" else -1.0
        return sign * PortfolioRiskEngine._position_notional(pos)

    def build_state(
        self,
        *,
        positions: dict[str, Any],
        equity: float,
        realized_pnl_today: float = 0.0,
        max_drawdown_proxy: float = 0.0,
    ) -> PortfolioState:
        symbol_exp: dict[str, float] = {}
        gross = 0.0
        net = 0.0
        for sym, pos in (positions or {}).items():
            n = self._position_notional(pos)
            gross += n
            net += self._position_signed_notional(pos)
            symbol_exp[str(sym)] = symbol_exp.get(str(sym), 0.0) + n
        return PortfolioState(
            equity=float(equity),
            open_positions=dict(positions or {}),
            gross_exposure_usdt=float(gross),
            net_exposure_usdt=float(net),
            symbol_exposure_usdt=symbol_exp,
            realized_pnl_today=float(realized_pnl_today),
            max_drawdown_proxy=float(max_drawdown_proxy),
        )

    @staticmethod
    def _corr(x: list[float], y: list[float]) -> float | None:
        n = min(len(x), len(y))
        if n < 3:
            return None
        xs = x[-n:]
        ys = y[-n:]
        mx = sum(xs) / n
        my = sum(ys) / n
        vx = sum((v - mx) ** 2 for v in xs)
        vy = sum((v - my) ** 2 for v in ys)
        if vx <= 0 or vy <= 0:
            return None
        cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
        return float(cov / math.sqrt(vx * vy))

    def evaluate_entry(
        self,
        *,
        symbol: str,
        side: str,
        qty_prelim: float,
        price: float,
        positions: dict[str, Any],
        equity: float,
        ts_ms: int,
        returns_windows: dict[str, list[float]] | None = None,
        realized_pnl_today: float = 0.0,
    ) -> PortfolioRiskDecision:
        del ts_ms
        notes: list[str] = []
        risk_scalar = 1.0
        state = self.build_state(
            positions=positions,
            equity=equity,
            realized_pnl_today=realized_pnl_today,
        )
        eq = max(float(equity), 1e-12)
        side_u = str(side or "").upper()
        signed_new = (1.0 if side_u == "BUY" else -1.0) * abs(float(qty_prelim) * float(price))
        new_notional = abs(float(qty_prelim) * float(price))

        if int(self.limits.max_concurrent_positions) >= 0 and len(state.open_positions) >= int(self.limits.max_concurrent_positions):
            return PortfolioRiskDecision(allow_entry=False, risk_scalar=0.0, note="max_concurrent_positions")

        if self.limits.daily_loss_limit_pct is not None:
            loss_limit = abs(float(self.limits.daily_loss_limit_pct)) * eq
            if loss_limit > 0 and float(realized_pnl_today) <= -loss_limit:
                return PortfolioRiskDecision(allow_entry=False, risk_scalar=0.0, note="daily_loss_limit")

        per_symbol_cap = max(0.0, float(self.limits.max_per_symbol_exposure_pct)) * eq
        existing_symbol = float(state.symbol_exposure_usdt.get(str(symbol), 0.0))
        if per_symbol_cap > 0 and existing_symbol + new_notional > per_symbol_cap:
            remain = max(0.0, per_symbol_cap - existing_symbol)
            if new_notional <= 0 or remain <= 0:
                return PortfolioRiskDecision(allow_entry=False, risk_scalar=0.0, note="per_symbol_cap")
            scale = remain / new_notional
            risk_scalar *= max(0.0, min(1.0, scale))
            notes.append("per_symbol_scale")

        gross_cap = max(0.0, float(self.limits.max_gross_exposure_pct)) * eq
        gross_after = float(state.gross_exposure_usdt + new_notional)
        if gross_cap > 0 and gross_after > gross_cap:
            remain = max(0.0, gross_cap - float(state.gross_exposure_usdt))
            if new_notional <= 0 or remain <= 0:
                return PortfolioRiskDecision(allow_entry=False, risk_scalar=0.0, note="gross_cap")
            scale = remain / new_notional
            risk_scalar *= max(0.0, min(1.0, scale))
            notes.append("gross_scale")

        net_cap = max(0.0, float(self.limits.max_net_exposure_pct)) * eq
        net_after_abs = abs(float(state.net_exposure_usdt + signed_new))
        if net_cap > 0 and net_after_abs > net_cap:
            current_net_abs = abs(float(state.net_exposure_usdt))
            remain = max(0.0, net_cap - current_net_abs)
            if abs(signed_new) <= 0 or remain <= 0:
                return PortfolioRiskDecision(allow_entry=False, risk_scalar=0.0, note="net_cap")
            scale = remain / abs(signed_new)
            risk_scalar *= max(0.0, min(1.0, scale))
            notes.append("net_scale")

        if self.limits.correlation_guard and returns_windows:
            sym_returns = returns_windows.get(str(symbol)) or []
            for open_sym in state.open_positions.keys():
                other = returns_windows.get(str(open_sym)) or []
                corr = self._corr(sym_returns, other)
                if corr is None:
                    continue
                if self.limits.correlation_block_threshold is not None and corr >= float(self.limits.correlation_block_threshold):
                    return PortfolioRiskDecision(allow_entry=False, risk_scalar=0.0, note=f"corr_block:{open_sym}:{corr:.2f}")
                if corr >= float(self.limits.correlation_threshold):
                    risk_scalar *= max(0.0, min(1.0, float(self.limits.correlation_risk_scalar)))
                    notes.append(f"corr_scale:{open_sym}:{corr:.2f}")
                    break

        risk_scalar = max(0.0, min(2.0, float(risk_scalar)))
        if risk_scalar <= 0.0:
            return PortfolioRiskDecision(allow_entry=False, risk_scalar=0.0, note="scale_zero")
        note = ",".join(notes) if notes else "ok"
        return PortfolioRiskDecision(allow_entry=True, risk_scalar=float(risk_scalar), note=note)
