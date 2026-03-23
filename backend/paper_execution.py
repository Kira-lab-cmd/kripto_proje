# File: backend/paper_execution.py
from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class PaperSlipConfig:
    base_slippage_bps: float
    jitter_bps: float


class PaperExecutionError(RuntimeError):
    pass


class PaperExecutor:
    """
    Deterministic paper execution engine (spot).
    Goals:
    - Idempotency: same idempotency_key will never double-apply balances.
    - Deterministic slippage: computed from idempotency_key (no randomness).
    - Full audit: persisted paper_orders table.

    Fee model:
    - Commission charged on notional in quote currency.
    Slippage model:
    - BUY executes at mid_price * (1 + slip)
    - SELL executes at mid_price * (1 - slip)
    where slip is in decimal (bps / 10_000).
    """

    def __init__(
        self,
        db: Any,
        commission_rate: float,
        slip_cfg: PaperSlipConfig,
    ) -> None:
        self._db = db
        self._commission_rate = float(commission_rate)
        self._slip_cfg = slip_cfg

    @staticmethod
    def _utc_iso() -> str:
        return datetime.utcnow().isoformat()

    @staticmethod
    def _hash_unit_interval(key: str) -> float:
        # Deterministic float in [0, 1)
        h = hashlib.sha256(key.encode("utf-8")).hexdigest()
        # 52 bits mantissa for stable float generation
        v = int(h[:13], 16)  # 13 hex ~= 52 bits
        return v / float(1 << 52)

    def _slippage_bps(self, idempotency_key: str) -> float:
        base = max(0.0, float(self._slip_cfg.base_slippage_bps))
        jitter = max(0.0, float(self._slip_cfg.jitter_bps))
        if jitter <= 0:
            return base

        # map u in [0,1) -> [-1, +1)
        u = self._hash_unit_interval("slip:" + idempotency_key)
        signed = (u * 2.0) - 1.0
        return max(0.0, base + signed * jitter)

    def execute_market_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        mid_price: float,
        idempotency_key: str,
        reason: str = "PAPER",
        audit_fields: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        side_u = side.upper().strip()
        if side_u not in ("BUY", "SELL"):
            raise ValueError("side must be BUY or SELL")

        amount_f = float(amount)
        price_f = float(mid_price)
        if amount_f <= 0 or price_f <= 0:
            raise ValueError("amount and mid_price must be > 0")

        if not idempotency_key or not idempotency_key.strip():
            raise ValueError("idempotency_key is required for paper execution")

        # Idempotency: return existing filled/rejected order (never double-apply)
        existing = self._db.get_paper_order_by_key(idempotency_key=idempotency_key)
        if existing is not None:
            return existing

        slip_bps = self._slippage_bps(idempotency_key)
        slip = slip_bps / 10_000.0

        exec_price = price_f * (1.0 + slip) if side_u == "BUY" else price_f * (1.0 - slip)
        notional = amount_f * exec_price
        fee = notional * float(self._commission_rate)

        # Persist order as NEW first
        order_id = self._db.create_paper_order(
            idempotency_key=idempotency_key,
            symbol=symbol,
            side=side_u,
            amount=amount_f,
            mid_price=price_f,
            exec_price=exec_price,
            fee=fee,
            slippage_bps=slip_bps,
            reason=reason,
            strategy_name=(audit_fields or {}).get("strategy_name"),
            entry_reason=(audit_fields or {}).get("entry_reason"),
            exit_reason=(audit_fields or {}).get("exit_reason"),
            regime=(audit_fields or {}).get("regime"),
            atr_pct=(audit_fields or {}).get("atr_pct"),
            dir_1h=(audit_fields or {}).get("dir_1h"),
            entry_price=exec_price if side_u == "BUY" else None,
            exit_price=exec_price if side_u == "SELL" else None,
            entry_cost=notional if side_u == "BUY" else None,
            exit_cost=notional if side_u == "SELL" else None,
            entry_fee=fee if side_u == "BUY" else None,
            exit_fee=fee if side_u == "SELL" else None,
            total_fees=fee,
            gross_pnl=None,
            net_pnl=None,
        )

        # Apply balances; if fail, mark REJECTED
        try:
            self._db.apply_paper_wallet_delta(
                symbol=symbol,
                side=side_u,
                amount=amount_f,
                exec_price=exec_price,
                fee=fee,
            )
        except Exception as e:
            self._db.set_paper_order_status(order_id=order_id, status="REJECTED", error=str(e))
            raise PaperExecutionError(str(e)) from e

        self._db.set_paper_order_status(order_id=order_id, status="FILLED", error=None)

        ts_ms = int(time.time() * 1000)
        return {
            "id": order_id,
            "clientOrderId": idempotency_key,
            "symbol": symbol,
            "side": side_u.lower(),
            "type": "market",
            "amount": amount_f,
            "filled": amount_f,
            "price": exec_price,
            "average": exec_price,
            "cost": notional,
            "fee": fee,
            "status": "closed",
            "timestamp": ts_ms,
            "info": {
                "mode": "paper",
                "mid_price": price_f,
                "slippage_bps": slip_bps,
                "reason": reason,
            },
        }
