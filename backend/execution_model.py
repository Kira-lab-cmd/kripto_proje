from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class ExecutionContext:
    symbol: str
    ts_ms: int
    action: str  # BUY/SELL
    qty: float
    ref_price: float
    atr_pct: float | None
    is_entry: bool
    side_position: str | None = None
    # Backward-compat aliases used by older callers.
    side: str | None = None
    mid_price: float | None = None
    regime: str | None = None
    vol_ratio: float | None = None
    trend_dir_1h: str | None = None


@dataclass(frozen=True)
class ExecutionFill:
    exec_price: float
    fee_paid: float
    slippage_bps: float
    fee_bps: float
    note: str | None = None
    filled_qty: float | None = None
    rejected: bool = False


class ExecutionModel(Protocol):
    def fill(self, ctx: ExecutionContext) -> ExecutionFill:
        ...


class DefaultExecutionModel(ExecutionModel):
    def __init__(
        self,
        *,
        taker_fee_bps: float = 10.0,
        maker_fee_bps: float = 6.0,
        base_slippage_bps: float = 2.0,
        slippage_atr_k: float = 1.0,
        max_slippage_bps: float | None = 15.0,
        min_notional: float = 0.0,
        step_size: float | None = None,
        tick_size: float | None = None,
    ):
        self.taker_fee_bps = float(taker_fee_bps)
        self.maker_fee_bps = float(maker_fee_bps)
        self.base_slippage_bps = float(base_slippage_bps)
        self.slippage_atr_k = float(slippage_atr_k)
        self.max_slippage_bps = float(max_slippage_bps) if max_slippage_bps is not None else None
        self.min_notional = max(0.0, float(min_notional))
        self.step_size = float(step_size) if step_size is not None else None
        self.tick_size = float(tick_size) if tick_size is not None else None

    def fill(self, ctx: ExecutionContext) -> ExecutionFill:
        side = str(ctx.action or ctx.side or "").upper()
        if side not in ("BUY", "SELL"):
            side = "BUY"
        atr_pct = max(0.0, float(ctx.atr_pct or 0.0))
        slippage_bps = float(self.base_slippage_bps + (self.slippage_atr_k * atr_pct * 10_000.0))
        if self.max_slippage_bps is not None:
            slippage_bps = max(0.0, min(slippage_bps, float(self.max_slippage_bps)))
        else:
            slippage_bps = max(0.0, slippage_bps)

        ref_price = float(ctx.ref_price if ctx.ref_price is not None else (ctx.mid_price if ctx.mid_price is not None else 0.0))
        if side == "BUY":
            exec_price = ref_price * (1.0 + slippage_bps / 10_000.0)
        else:
            exec_price = ref_price * (1.0 - slippage_bps / 10_000.0)

        if self.tick_size is not None and self.tick_size > 0:
            exec_price = round(exec_price / self.tick_size) * self.tick_size

        fee_bps = float(self.taker_fee_bps)
        qty = float(ctx.qty)
        if self.step_size is not None and self.step_size > 0:
            qty = math.floor(qty / self.step_size) * self.step_size
        notional = abs(exec_price * qty)
        if qty <= 0.0 or (self.min_notional > 0 and notional < self.min_notional):
            return ExecutionFill(
                exec_price=float(exec_price),
                fee_paid=0.0,
                slippage_bps=float(slippage_bps),
                fee_bps=float(fee_bps),
                note="rejected:min_notional_or_step",
                filled_qty=0.0,
                rejected=True,
            )
        fee_paid = notional * (fee_bps / 10_000.0)
        note = f"slip_bps={slippage_bps:.3f},fee_bps={fee_bps:.3f}"
        return ExecutionFill(
            exec_price=float(exec_price),
            fee_paid=float(fee_paid),
            slippage_bps=float(slippage_bps),
            fee_bps=float(fee_bps),
            note=note,
            filled_qty=float(qty),
            rejected=False,
        )


# Backward compatibility shim for existing imports/callers.
@dataclass(frozen=True)
class FillResult:
    side: str
    order_type: str
    ref_price: float
    fill_price: float
    qty: float
    notional: float
    fee: float
    fee_bps: float
    slippage_bps: float
    slippage_cost: float
    spread_cost: float
    fill_ratio: float
    is_maker: bool
    note: str = ""


class ExecutionEngine:
    def __init__(self, model: ExecutionModel | None = None):
        self.model = model or DefaultExecutionModel()

    @classmethod
    def from_basic(
        cls,
        *,
        fee_bps_maker: float = 10.0,
        fee_bps_taker: float = 10.0,
        slippage_bps: float = 2.0,
        seed: int = 7,
        min_notional: float = 0.0,
        step_size: float | None = None,
        tick_size: float | None = None,
    ) -> "ExecutionEngine":
        _ = int(seed)
        model = DefaultExecutionModel(
            taker_fee_bps=float(fee_bps_taker),
            maker_fee_bps=float(fee_bps_maker),
            base_slippage_bps=float(slippage_bps),
            slippage_atr_k=0.0,
            max_slippage_bps=float(slippage_bps),
            min_notional=float(min_notional),
            step_size=step_size,
            tick_size=tick_size,
        )
        return cls(model=model)

    @classmethod
    def from_realistic(
        cls,
        *,
        fee_bps_maker: float = 10.0,
        fee_bps_taker: float = 10.0,
        slippage_bps: float = 2.0,
        seed: int = 7,
        min_notional: float = 0.0,
        step_size: float | None = None,
        tick_size: float | None = None,
    ) -> "ExecutionEngine":
        _ = int(seed)
        model = DefaultExecutionModel(
            taker_fee_bps=float(fee_bps_taker),
            maker_fee_bps=float(fee_bps_maker),
            base_slippage_bps=float(slippage_bps),
            slippage_atr_k=1.0,
            max_slippage_bps=15.0,
            min_notional=float(min_notional),
            step_size=step_size,
            tick_size=tick_size,
        )
        return cls(model=model)

    def execute_market(
        self,
        *,
        side: str,
        ref_price: float,
        qty: float,
        is_maker: bool = False,
        note: str = "",
    ) -> FillResult:
        _ = bool(is_maker)
        ctx = ExecutionContext(
            symbol="",
            ts_ms=0,
            action=str(side),
            qty=float(qty),
            ref_price=float(ref_price),
            atr_pct=None,
            is_entry=True,
            side_position=None,
        )
        fill = self.model.fill(ctx)
        filled_qty = float(fill.filled_qty if fill.filled_qty is not None else qty)
        notional = abs(float(fill.exec_price) * filled_qty)
        slippage_cost = abs(float(fill.exec_price) - float(ref_price)) * abs(filled_qty)
        fill_ratio = float(filled_qty / float(qty)) if float(qty) > 0 else 0.0
        note_parts = [x for x in [str(note or ""), str(fill.note or "")] if x]
        return FillResult(
            side=str(side).upper(),
            order_type="MARKET",
            ref_price=float(ref_price),
            fill_price=float(fill.exec_price),
            qty=float(filled_qty),
            notional=float(notional),
            fee=float(fill.fee_paid),
            fee_bps=float(fill.fee_bps),
            slippage_bps=float(fill.slippage_bps),
            slippage_cost=float(slippage_cost),
            spread_cost=0.0,
            fill_ratio=fill_ratio,
            is_maker=False,
            note=" | ".join(note_parts),
        )
