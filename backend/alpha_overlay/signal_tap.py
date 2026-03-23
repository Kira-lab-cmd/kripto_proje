from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class SignalTap:
    regime: str | None = None
    adx: float | None = None
    er: float | None = None
    atr_pct: float | None = None
    trend_dir_1h: str | None = None
    score: float | None = None
    buy_th: float | None = None
    sell_th: float | None = None
    reason: str | None = None

    @classmethod
    def from_strategy_res(cls, res: dict[str, Any] | None) -> "SignalTap":
        data = res or {}
        eff = data.get("effective_thresholds") or {}
        buy_th = data.get("buy_th")
        sell_th = data.get("sell_th")
        if buy_th is None:
            buy_th = eff.get("buy")
        if sell_th is None:
            sell_th = eff.get("sell")
        return cls(
            regime=data.get("regime"),
            adx=data.get("adx"),
            er=data.get("er"),
            atr_pct=data.get("atr_pct"),
            trend_dir_1h=data.get("dir_1h") or data.get("trend_dir_1h"),
            score=data.get("score"),
            buy_th=buy_th,
            sell_th=sell_th,
            reason=data.get("reason"),
        )

    def apply_to_position(self, pos: Any) -> None:
        mapping = {
            "entry_regime": self.regime,
            "entry_adx": self.adx,
            "entry_er": self.er,
            "entry_atr_pct": self.atr_pct,
            "entry_trend_dir_1h": self.trend_dir_1h,
            "entry_score": self.score,
            "entry_buy_th": self.buy_th,
            "entry_sell_th": self.sell_th,
            "entry_reason": self.reason,
        }
        for attr, value in mapping.items():
            if hasattr(pos, attr):
                setattr(pos, attr, value)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
