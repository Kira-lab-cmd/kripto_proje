# File: backend/trader.py
from __future__ import annotations

import hashlib
import logging
import os
import time
from typing import Any

from .alpha_overlay import OverlayFeatureFlags, PortfolioRisk, SignalTap, log_trade_close
from .config import settings
from .paper_execution import PaperExecutor, PaperSlipConfig, PaperExecutionError
from .risk_engine import compute_qty_from_stop

logger = logging.getLogger(__name__)


class TradingPausedError(RuntimeError):
    pass


class Trader:
    """
    Spot trade executor.

    TEST_MODE:
      - Deterministic, idempotent paper execution (balances + audit in SQLite).
    LIVE:
      - ccxt market orders.

    Safety:
      - In paper mode we ALWAYS require an idempotency_key to prevent duplicate execution.
      - In live mode you should still enforce idempotency at the exchange (clientOrderId) level
        (not implemented yet; next sprint item).
    """

    def __init__(
        self,
        exchange: Any,
        db: Any,
        notifier: Any,
        test_mode: bool = True,
        risk_per_trade: float = 0.05,
    ) -> None:
        self.exchange = exchange
        self.db = db
        self.notifier = notifier
        self.test_mode = bool(test_mode)
        self.risk_per_trade = float(risk_per_trade)
        self._overlay_flags = OverlayFeatureFlags.from_env()
        self._entry_signal_taps: dict[str, SignalTap] = {}

        self.commission_rate = float(os.getenv("COMMISSION_RATE", str(settings.COMMISSION_RATE)))
        self._idempotency_window_seconds = int(os.getenv("COOLDOWN_SECONDS", str(settings.COOLDOWN_SECONDS)) or "300")

        self._paper = PaperExecutor(
            db=self.db,
            commission_rate=self.commission_rate,
            slip_cfg=PaperSlipConfig(
                base_slippage_bps=float(os.getenv("PAPER_SLIPPAGE_BPS", str(settings.PAPER_SLIPPAGE_BPS))),
                jitter_bps=float(os.getenv("PAPER_SLIPPAGE_JITTER_BPS", str(settings.PAPER_SLIPPAGE_JITTER_BPS))),
            ),
        )

        # TEST_MODE: ensure initial USDT exists; only sync if clean state (no trades, no positions)
        if self.test_mode:
            paper_usdt = float(os.getenv("PAPER_USDT", str(settings.PAPER_USDT)))
            try:
                self.db.ensure_paper_asset("USDT", initial_free=paper_usdt)
                try:
                    summary = self.db.get_dashboard_summary()
                    trade_count = int(summary.get("trade_count", 0))
                    open_positions = int(summary.get("open_positions", 0))
                except Exception:
                    trade_count, open_positions = 0, 0

                if trade_count == 0 and open_positions == 0:
                    current = float(self.db.get_paper_balance("USDT"))
                    if abs(current - paper_usdt) > 1e-9:
                        self.db.set_paper_balance("USDT", paper_usdt)
                        logger.info("🧪 PAPER_USDT senkronlandı: %.2f -> %.2f", current, paper_usdt)
            except Exception as e:
                logger.error("PAPER init hatası: %s", e)

    def _build_signal_tap(self, signal_details: dict[str, Any] | None) -> SignalTap | None:
        if not signal_details:
            return None
        try:
            return SignalTap.from_strategy_res(signal_details)
        except Exception:
            logger.exception("overlay signal tap build failed")
            return None

    @staticmethod
    def _as_optional_text(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _build_trade_audit_fields(
        self,
        *,
        signal: str,
        reason: str,
        signal_details: dict[str, Any] | None,
        position_before_sell: dict[str, Any] | None,
        entry_tap: SignalTap | None,
    ) -> dict[str, Any]:
        signal_u = str(signal).upper().strip()
        if signal_u == "BUY":
            data = signal_details or {}
            return {
                "strategy_name": self._as_optional_text(data.get("strategy_name")),
                "entry_reason": self._as_optional_text(data.get("reason")),
                "exit_reason": None,
                "regime": self._as_optional_text(data.get("regime")),
                "atr_pct": data.get("atr_pct"),
                "dir_1h": self._as_optional_text(data.get("dir_1h") or data.get("trend_dir_1h")),
            }

        data = position_before_sell or {}
        return {
            "strategy_name": self._as_optional_text(data.get("strategy_name") or (signal_details or {}).get("strategy_name")),
            "entry_reason": self._as_optional_text(data.get("entry_reason") or (entry_tap.reason if entry_tap else None)),
            "exit_reason": self._as_optional_text(reason),
            "regime": self._as_optional_text(data.get("regime") or (entry_tap.regime if entry_tap else None)),
            "atr_pct": data.get("atr_pct", entry_tap.atr_pct if entry_tap else None),
            "dir_1h": self._as_optional_text(data.get("dir_1h") or (entry_tap.trend_dir_1h if entry_tap else None)),
        }

    def _attach_order_overlay(self, order: dict[str, Any] | None, tap: SignalTap | None) -> None:
        if not order or not tap or not self._overlay_flags.signal_tap:
            return
        try:
            order["overlay"] = {"entry_signal_tap": tap.as_dict()}
        except Exception:
            logger.exception("overlay order attach failed")

    @staticmethod
    def _compute_r_multiple(position_before_sell: dict[str, Any] | None, sold_amount: float, realized_pnl: float) -> float | None:
        if not position_before_sell or sold_amount <= 0:
            return None
        try:
            entry_price = float(position_before_sell.get("entry_price") or 0.0)
            stop_loss = position_before_sell.get("stop_loss")
            if stop_loss is None:
                return None
            risk_usdt = abs(entry_price - float(stop_loss)) * float(sold_amount)
            if risk_usdt <= 0:
                return None
            return float(realized_pnl) / risk_usdt
        except Exception:
            return None

    # -------------------------
    # Market guards (precision/min-notional)
    # -------------------------
    def _market(self, symbol: str) -> dict:
        try:
            return self.exchange.market(symbol)
        except Exception:
            try:
                return (self.exchange.markets or {}).get(symbol) or {}
            except Exception:
                return {}

    def _min_amount(self, symbol: str) -> float:
        m = self._market(symbol)
        try:
            v = (((m.get("limits") or {}).get("amount") or {}).get("min"))
            return float(v) if v is not None else 0.0
        except Exception:
            return 0.0

    def _min_cost(self, symbol: str) -> float:
        m = self._market(symbol)
        try:
            v = (((m.get("limits") or {}).get("cost") or {}).get("min"))
            return float(v) if v is not None else 0.0
        except Exception:
            return 0.0

    def _normalize_amount(self, symbol: str, amount: float) -> float:
        try:
            amt_str = self.exchange.amount_to_precision(symbol, float(amount))
            return float(amt_str)
        except Exception:
            try:
                return float(amount)
            except Exception:
                return 0.0

    def _preflight_order(self, symbol: str, amount: float, price: float, is_emergency: bool = False) -> tuple[float, float] | None:
        if amount <= 0 or price <= 0:
            return None

        amt = self._normalize_amount(symbol, amount)
        if amt <= 0:
            logger.warning("⛔ amount precision sonrası 0 oldu: raw=%s -> %s (%s)", amount, amt, symbol)
            return None

        min_amt = self._min_amount(symbol)
        if min_amt > 0 and amt < min_amt:
            if is_emergency:
                # Emergency: min_amount'a yükselt, bloklama
                logger.warning("emergency_preflight_min_amount sym=%s amt=%.8f → %.8f", symbol, amt, min_amt)
                amt = min_amt
            else:
                logger.warning("⛔ min_amount guard: amt=%s < min=%s (%s)", amt, min_amt, symbol)
                return None

        notional = amt * float(price)
        min_cost = self._min_cost(symbol)

        if is_emergency:
            # Emergency SELL: min_cost ve notional guard'ları atla — pozisyon kapatılmalı
            if notional < 0.01:
                logger.warning("⛔ emergency notional sıfıra yakın: %.6f (%s) — atlandı", notional, symbol)
                return None
            return amt, notional

        if min_cost > 0 and notional < min_cost:
            logger.warning("⛔ min_cost guard: notional=%s < min=%s (%s)", notional, min_cost, symbol)
            return None

        if notional < 5.0:
            logger.warning("⛔ hard min notional guard: notional=%s < 5.0 (%s) — devam ediliyor", notional, symbol)

        return amt, notional

    def _pause_after_trade_persist_failure(self, *, symbol: str, side: str, error: Exception) -> None:
        logger.exception(
            "DB add_trade failed -> PAUSE symbol=%s side=%s err=%s",
            symbol,
            side,
            error,
        )
        try:
            self.db.set_bot_enabled(False)
        except Exception:
            logger.exception("set_bot_enabled(False) failed after DB add_trade failure")
        try:
            if self.notifier and getattr(self.notifier, "enabled", False):
                self.notifier.notify_paused_alert(
                    reason="DB persistence failed",
                    detail=f"symbol={symbol} side={side} err={str(error)[:300]}",
                )
        except Exception:
            logger.exception("send_paused_alert failed after DB add_trade failure")
        raise TradingPausedError("DB persistence failed; trading paused for safety.") from error

    # -------------------------
    # Basic market data
    # -------------------------
    def get_balance(self, asset: str) -> float:
        asset_u = asset.upper().strip()
        if self.test_mode:
            return float(self.db.get_paper_balance(asset_u))

        try:
            balances = self.exchange.fetch_balance()
            free = ((balances.get("free") or {}).get(asset_u))
            return float(free) if free is not None else 0.0
        except Exception:
            return 0.0

    def get_ticker_price(self, symbol: str) -> float:
        """
        Robust-ish ticker price getter (sync):
          - logs on failure
          - returns 0.0 if unavailable
        NOTE: BinanceService has stronger failover. Prefer using it for prices if available.
        """
        try:
            t = self.exchange.fetch_ticker(symbol)
            p = t.get("last") or t.get("close")
            return float(p) if p else 0.0
        except Exception as e:
            logger.error("Fiyat çekme hatası (Trader.get_ticker_price %s): %s", symbol, str(e)[:220])
            return 0.0

    # -------------------------
    # Orders
    # -------------------------
    @staticmethod
    def _make_idempotency_key(
        symbol: str,
        side: str,
        reason: str,
        mid_price: float,
        window_bucket: int,
    ) -> str:
        raw = f"{symbol}|{side}|{reason}|{round(float(mid_price), 8)}|{window_bucket}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def place_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float | None = None,
        *,
        idempotency_key: str | None = None,
        reason: str = "AUTO_SIGNAL",
        audit_fields: dict[str, Any] | None = None,
    ) -> dict | None:
        """
        TEST_MODE:
          - deterministic market fill with slippage + fee
          - persisted order audit + idempotency
        LIVE:
          - market order via ccxt
        """
        try:
            amount_n = self._normalize_amount(symbol, amount)
            if amount_n <= 0:
                logger.warning("⛔ place_order: amount precision sonrası 0: (%s)", symbol)
                return None

            mid_price = float(price) if price else self.get_ticker_price(symbol)
            if mid_price <= 0:
                return None

            if self.test_mode:
                if not idempotency_key:
                    bucket = int(time.time() // max(1, self._idempotency_window_seconds))
                    idempotency_key = self._make_idempotency_key(
                        symbol=symbol,
                        side=side.upper(),
                        reason=reason,
                        mid_price=mid_price,
                        window_bucket=bucket,
                    )

                order = self._paper.execute_market_order(
                    symbol=symbol,
                    side=side.upper(),
                    amount=float(amount_n),
                    mid_price=mid_price,
                    idempotency_key=idempotency_key,
                    reason=reason,
                    audit_fields=audit_fields,
                )

                logger.info(
                    "🧪 [PAPER] %s %s %s @ mid=%.6f exec=%.6f fee=%.6f slip_bps=%.3f id=%s",
                    side.upper(),
                    amount_n,
                    symbol,
                    float(order["info"]["mid_price"]),
                    float(order["average"]),
                    float(order["fee"]),
                    float(order["info"]["slippage_bps"]),
                    order["id"],
                )
                return order

            # LIVE
            if side == "buy":
                return self.exchange.create_market_buy_order(symbol, amount_n)
            if side == "sell":
                return self.exchange.create_market_sell_order(symbol, amount_n)
            return None

        except PaperExecutionError as e:
            logger.error("❌ PAPER ORDER REJECTED (%s %s): %s", symbol, side, e)
            return None
        except Exception as e:
            logger.error("❌ EMİR HATASI (%s %s): %s", symbol, side, e)
            return None

    # -------------------------
    # Main execution
    # -------------------------
    def execute_trade(
        self,
        symbol: str,
        signal: str,
        current_price: float,
        risk_multiplier: float = 1.0,
        reason: str = "AUTO_SIGNAL",
        stop_loss: float | None = None,
        take_profit: float | None = None,
        *,
        signal_id: str | None = None,
        equity_usdt: float | None = None,
        signal_details: dict[str, Any] | None = None,
    ) -> dict | None:
        if signal not in ["BUY", "SELL"]:
            return None

        # Fail-closed: if bot is disabled, surface pause (machine-readable via API handler).
        try:
            st = self.db.get_bot_state() if self.db else None
            if isinstance(st, dict) and (not bool(st.get("is_enabled", True))):
                raise TradingPausedError("Bot is disabled; trading paused.")
        except TradingPausedError:
            raise
        except Exception:
            # If state cannot be read, do NOT assume enabled.
            raise TradingPausedError("Bot state unreadable; trading paused for safety.")

        risk_multiplier = max(0.0, min(1.0, float(risk_multiplier)))
        if risk_multiplier <= 0:
            logger.warning("⛔ risk_multiplier=0 => trade iptal")
            return None

        if not current_price or current_price <= 0:
            logger.warning("⛔ current_price invalid => trade iptal")
            return None

        base_currency, quote_currency = symbol.split("/")
        base_currency = base_currency.upper().strip()
        quote_currency = quote_currency.upper().strip()

        amount_to_trade = 0.0
        position_before_sell: dict[str, Any] | None = None
        signal_tap = self._build_signal_tap(signal_details)
        entry_tap = self._entry_signal_taps.get(symbol)

        # Concentration guard: cap any single entry notional as % of equity.
        # Keeps tail risk bounded even if SL distance is tiny.
        try:
            max_notional_pct = float(os.getenv("MAX_NOTIONAL_PCT", "0.25"))
            if max_notional_pct <= 0 or max_notional_pct > 1:
                max_notional_pct = 0.25
        except Exception:
            max_notional_pct = 0.25

        if signal == "BUY":
            quote_balance = self.get_balance(quote_currency)
            if quote_balance < 10:
                logger.warning("Yetersiz Bakiye (%s): %.2f", quote_currency, quote_balance)
                return None

            eq = float(equity_usdt) if equity_usdt is not None else float(quote_balance)

            strategy_name = None
            if signal_details:
                strategy_name = signal_details.get("strategy_name")

            if strategy_name == "grid_v1":
                # GRID STRATEGY: Coin bazlı sermaye
                # Her coin için ayrı env var, yoksa genel default
                _coin_capital_map = {
                    "BTC": float(os.getenv("GRID_CAPITAL_BTC", "0")),
                    "ETH": float(os.getenv("GRID_CAPITAL_ETH", "0")),
                    "SOL": float(os.getenv("GRID_CAPITAL_SOL", "0")),
                    "NEAR": float(os.getenv("GRID_CAPITAL_NEAR", "0")),
                    "BNB": float(os.getenv("GRID_CAPITAL_BNB", "0")),
                    "XRP": float(os.getenv("GRID_CAPITAL_XRP", "0")),
                }
                _default_capital = float(os.getenv("GRID_CAPITAL_PER_LEVEL", "20.0"))
                _coin_specific = _coin_capital_map.get(base_currency, 0.0)
                grid_capital_per_level = _coin_specific if _coin_specific > 0 else _default_capital
                amount_to_trade = grid_capital_per_level / float(current_price)

                logger.info(
                    "🔧 GRID POSITION SIZING: capital=$%.2f price=$%.2f qty=%.8f %s strategy=%s",
                    grid_capital_per_level,
                    float(current_price),
                    amount_to_trade,
                    base_currency,
                    strategy_name,
                )
            else:
                # NORMAL STRATEGIES: Risk-based sizing
                risk_pct = float(self.risk_per_trade) * float(risk_multiplier)
                if self._overlay_flags.portfolio_risk:
                    risk_pct = PortfolioRisk.adjust_risk_budget(
                        base_risk=float(risk_pct),
                        equity=float(eq),
                        dd_pct=None,
                        regime=(signal_tap.regime if signal_tap else None),
                        atr_pct=(signal_tap.atr_pct if signal_tap else None),
                        corr_exposure=None,
                    )

                sr = compute_qty_from_stop(
                    symbol=symbol,
                    entry_price=float(current_price),
                    stop_loss=stop_loss,
                    equity_usdt=float(eq),
                    risk_pct=float(risk_pct),
                    exchange=self.exchange,
                    max_notional_pct=float(max_notional_pct),
                )
                amount_to_trade = float(sr.qty)

                logger.info(
                    "📊 RISK-BASED SIZING: equity=$%.2f risk_pct=%.4f qty=%.8f %s strategy=%s",
                    float(eq),
                    float(risk_pct),
                    amount_to_trade,
                    base_currency,
                    strategy_name or "unknown",
                )

        else:  # SELL
            try:
                position_before_sell = self.db.get_open_position(symbol)
            except Exception:
                position_before_sell = None

            if not position_before_sell:
                logger.info(
                    "trade_blocked symbol=%s decision=SELL blocked_reason=no_open_position reason=%s",
                    symbol,
                    str(reason)[:120],
                )
                return None

            # Spot-safe: do NOT open shorts
            if self.test_mode:
                base_balance = float(position_before_sell["amount"])
            else:
                base_balance = self.get_balance(base_currency)

            value_in_quote = base_balance * float(current_price)

            # Grid SELL için eşiği düşür: grid_qty bazında kontrol et
            # Sorun: birden fazla grid seviyesi satıldıkça base_balance küçülüyor
            # ve 5.5 eşiğinin altına düşüp bloklanıyor.
            # crossing_metadata varsa (grid SELL sinyali) → grid_qty * fiyat kullan
            _crossing_check = (signal_details or {}).get("crossing_metadata", {})
            _grid_qty_check = float(_crossing_check.get("quantity", 0)) if _crossing_check else 0.0
            # Stop-loss, panic, exit sinyalleri için miktar kontrolünü atla
            # Acil kapatma sinyallerinde (stop-loss, trend-close, exit engine) miktar
            # kontrolünü atla — pozisyon kapatılabilmeli, balance küçük olsa bile
            _is_emergency = (
                reason in ["PANIC_CLOSE_ALL", "MANUAL_CLOSE", "STOP_LOSS",
                           "TAKE_PROFIT", "GRID_EMERGENCY_EXIT"]
                or (reason or "").startswith("EXIT_STOP_LOSS")
                or (reason or "").startswith("GRID_TREND_CLOSE")
                or (reason or "").startswith("STOP_LOSS")
                or (reason or "").startswith("TAKE_PROFIT")
                or (reason or "").startswith("EXIT_TRAIL")
                or (reason or "").startswith("EXIT_RSI_DECAY")
                or bool((signal_details or {}).get("_is_emergency"))
            )
            if _is_emergency:
                pass  # Acil kapatmalarda miktar kontrolü yok
            elif _grid_qty_check > 0:
                # Grid seviyesi SELL: gerçek işlem miktarı grid_qty
                _min_val = _grid_qty_check * float(current_price)
                if _min_val < 1.0:
                    logger.warning("Satılacak miktar çok düşük (grid): %.2f", _min_val)
                    return None
            elif value_in_quote < 5.5:
                logger.warning("Satılacak miktar çok düşük: %.2f", value_in_quote)
                return None

            strategy_name = None
            if signal_details:
                strategy_name = signal_details.get("strategy_name")

            if strategy_name == "grid_v1" and signal_details:
                crossing = signal_details.get("crossing_metadata", {})
                grid_qty = float(crossing.get("quantity", 0))

                if grid_qty > 0:
                    # Grid SELL: her zaman grid_qty kullan
                    # base_balance DB sync gecikmesinden küçük görünebilir
                    # grid_qty zaten o seviyenin kayıtlı miktarı
                    amount_to_trade = grid_qty
                    logger.info(
                        "🔧 GRID SELL SIZING: grid_qty=%.8f base_balance=%.8f %s",
                        grid_qty,
                        base_balance,
                        symbol,
                    )
                else:
                    amount_to_trade = base_balance
                    logger.info(
                        "🔧 GRID SELL FALLBACK: grid_qty=0, selling all base=%.8f %s",
                        base_balance,
                        symbol,
                    )
            elif reason in ["PANIC_CLOSE_ALL", "MANUAL_CLOSE", "STOP_LOSS", "TAKE_PROFIT"]:
                amount_to_trade = base_balance
            elif _is_emergency:
                # Trend-close, exit-engine vb. acil kapatmalar — tamamını sat
                amount_to_trade = base_balance
            else:
                amount_to_trade = base_balance * self.risk_per_trade * risk_multiplier

            logger.info(
                "SELL PREP symbol=%s strategy=%s base_balance=%.8f amount_to_trade=%.8f price=%.8f",
                symbol,
                strategy_name,
                base_balance,
                amount_to_trade,
                float(current_price),
            )

        pf = self._preflight_order(
            symbol,
            amount=amount_to_trade,
            price=float(current_price),
            is_emergency=(signal == "SELL" and _is_emergency)
        )
        if pf is None:
            return None
        
        
        amount_to_trade_n, notional = pf
        logger.debug(
            "PRECHECK OK symbol=%s signal=%s raw_amount=%.8f normalized_amount=%.8f notional=%.8f",
            symbol,
            signal,
            amount_to_trade,
            amount_to_trade_n,
            notional,
        )
        if self.test_mode and signal == "BUY":
            est_fee = notional * self.commission_rate
            quote_balance = self.get_balance(quote_currency)
            if (notional + est_fee) > quote_balance + 1e-9:
                logger.warning(
                    "⛔ PAPER insufficient %s: need=%.6f, have=%.6f (%s)",
                    quote_currency,
                    (notional + est_fee),
                    quote_balance,
                    symbol,
                )
                return None

        side = "buy" if signal == "BUY" else "sell"
        logger.info("⚡ Trade: %s %.8f %s | risk_x=%.3f | reason=%s", signal, amount_to_trade_n, symbol, risk_multiplier, reason)

        idempotency_key: str | None = None
        if self.test_mode:
            if signal_id:
                idempotency_key = hashlib.sha256(f"sig:{signal_id}".encode("utf-8")).hexdigest()
            else:
                bucket = int(time.time() // max(1, self._idempotency_window_seconds))
                idempotency_key = self._make_idempotency_key(
                    symbol=symbol,
                    side=signal,
                    reason=reason,
                    mid_price=float(current_price),
                    window_bucket=bucket,
                )

        trade_audit_fields = self._build_trade_audit_fields(
            signal=signal,
            reason=reason,
            signal_details=signal_details,
            position_before_sell=position_before_sell,
            entry_tap=entry_tap,
        )

        order = self.place_order(
            symbol=symbol,
            side=side,
            amount=amount_to_trade_n,
            price=float(current_price),
            idempotency_key=idempotency_key,
            reason=reason,
            audit_fields=trade_audit_fields,
        )
        if not order:
            logger.warning(
                "trade place_order returned_none symbol=%s signal=%s reason=%s amount=%.8f price=%.8f test_mode=%s",
                symbol,
                signal,
                reason,
                float(amount_to_trade_n),
                float(current_price),
                self.test_mode,
            )
            return None

        exec_price = float(order.get("average") or order.get("price") or current_price)
        exec_amount = float(order.get("filled") or order.get("amount") or amount_to_trade_n)
        trade_cost = float(exec_amount) * float(exec_price)
        logger.debug(
            "trade order_filled symbol=%s signal=%s order_id=%s exec_amount=%.8f exec_price=%.8f cost=%.8f",
            symbol,
            signal,
            order.get("id"),
            float(exec_amount),
            float(exec_price),
            float(trade_cost),
        )

        fee = float(order.get("fee") or 0.0) if self.test_mode else (exec_amount * exec_price * self.commission_rate)

        realized_pnl = 0.0
        position_audit: dict[str, Any] = {
            "entry_price": None,
            "exit_price": None,
            "entry_cost": None,
            "exit_cost": None,
            "entry_fee": None,
            "exit_fee": None,
            "total_fees": None,
            "gross_pnl": None,
            "net_pnl": None,
            "realized_pnl": 0.0,
        }
        try:
            position_audit = dict(
                self.db.update_position_audit(
                    symbol=symbol,
                    side=signal,
                    amount=exec_amount,
                    price=exec_price,
                    fee=fee,
                )
                or {}
            )
            realized_pnl = float(position_audit.get("realized_pnl") or 0.0)
            logger.debug(
                "trade position_updated symbol=%s signal=%s exec_amount=%.8f exec_price=%.8f fee=%.8f realized_pnl=%.8f gross_pnl=%s total_fees=%s net_pnl=%s",
                symbol,
                signal,
                float(exec_amount),
                float(exec_price),
                float(fee),
                float(realized_pnl),
                position_audit.get("gross_pnl"),
                position_audit.get("total_fees"),
                position_audit.get("net_pnl"),
            )
        except Exception as e:
            logger.error("DB update_position hatası: %s", e)

        if signal == "BUY":
            if self._overlay_flags.signal_tap and signal_tap is not None:
                self._entry_signal_taps[symbol] = signal_tap
                entry_tap = signal_tap
                self._attach_order_overlay(order, signal_tap)
        elif self._overlay_flags.signal_tap and entry_tap is not None:
            self._attach_order_overlay(order, entry_tap)

        if signal == "SELL" and position_before_sell:
            try:
                expected_realized_pnl = float(position_audit.get("net_pnl") or 0.0)
                if abs(float(realized_pnl) - float(expected_realized_pnl)) > 1e-8:
                    logger.warning(
                        "SELL realized_pnl mismatch symbol=%s db=%.8f expected_net=%.8f entry_cost=%s exit_cost=%s entry_fee=%s exit_fee=%s",
                        symbol,
                        float(realized_pnl),
                        float(expected_realized_pnl),
                        position_audit.get("entry_cost"),
                        position_audit.get("exit_cost"),
                        position_audit.get("entry_fee"),
                        position_audit.get("exit_fee"),
                    )
            except Exception:
                pass

        if self.test_mode and order.get("id"):
            try:
                self.db.update_paper_order_audit(
                    order_id=str(order["id"]),
                    strategy_name=trade_audit_fields.get("strategy_name"),
                    entry_reason=trade_audit_fields.get("entry_reason"),
                    exit_reason=trade_audit_fields.get("exit_reason"),
                    regime=trade_audit_fields.get("regime"),
                    atr_pct=trade_audit_fields.get("atr_pct"),
                    dir_1h=trade_audit_fields.get("dir_1h"),
                    entry_price=position_audit.get("entry_price"),
                    exit_price=position_audit.get("exit_price"),
                    entry_cost=position_audit.get("entry_cost"),
                    exit_cost=position_audit.get("exit_cost"),
                    entry_fee=position_audit.get("entry_fee"),
                    exit_fee=position_audit.get("exit_fee"),
                    total_fees=position_audit.get("total_fees"),
                    gross_pnl=position_audit.get("gross_pnl"),
                    net_pnl=position_audit.get("net_pnl"),
                )
            except Exception as e:
                logger.error("DB update_paper_order_audit hatası: %s", e)

        try:
            trade_id = self.db.add_trade(
                symbol=symbol,
                side=signal,
                timestamp=None,
                amount=exec_amount,
                price=exec_price,
                cost=trade_cost,
                fee=fee,
                realized_pnl=(float(realized_pnl) if signal == "SELL" else 0.0),
                strategy_name=trade_audit_fields.get("strategy_name"),
                entry_reason=trade_audit_fields.get("entry_reason"),
                exit_reason=trade_audit_fields.get("exit_reason"),
                regime=trade_audit_fields.get("regime"),
                atr_pct=trade_audit_fields.get("atr_pct"),
                dir_1h=trade_audit_fields.get("dir_1h"),
                entry_price=position_audit.get("entry_price"),
                exit_price=position_audit.get("exit_price"),
                entry_cost=position_audit.get("entry_cost"),
                exit_cost=position_audit.get("exit_cost"),
                entry_fee=position_audit.get("entry_fee"),
                exit_fee=position_audit.get("exit_fee"),
                total_fees=position_audit.get("total_fees"),
                gross_pnl=position_audit.get("gross_pnl"),
                net_pnl=position_audit.get("net_pnl"),
            )
            logger.debug(
                "trade audit_persisted trade_id=%s symbol=%s side=%s realized_pnl=%.8f fee=%.8f total_fees=%s gross_pnl=%s net_pnl=%s",
                trade_id,
                symbol,
                signal,
                float(realized_pnl) if signal == "SELL" else 0.0,
                float(fee),
                position_audit.get("total_fees"),
                position_audit.get("gross_pnl"),
                position_audit.get("net_pnl"),
            )
        except Exception as e:
            self._pause_after_trade_persist_failure(symbol=symbol, side=signal, error=e)
            logger.error("DB add_trade hatası: %s", e)

        if signal == "SELL" and self._overlay_flags.telemetry:
            sold_amount = min(float(exec_amount), float(position_before_sell.get("amount") or 0.0)) if position_before_sell else float(exec_amount)
            log_trade_close(
                {
                    "symbol": symbol,
                    "side": signal,
                    "pnl": float(realized_pnl),
                    "r_multiple": self._compute_r_multiple(position_before_sell, sold_amount, realized_pnl),
                    "regime": (entry_tap.regime if entry_tap else None),
                    "score": (entry_tap.score if entry_tap else None),
                    "buy_th": (entry_tap.buy_th if entry_tap else None),
                    "sell_th": (entry_tap.sell_th if entry_tap else None),
                    "reason": (entry_tap.reason if entry_tap else None),
                }
            )

        if signal == "SELL" and symbol in self._entry_signal_taps:
            self._entry_signal_taps.pop(symbol, None)

        if signal == "BUY":
            try:
                if stop_loss is not None or take_profit is not None:
                    self.db.set_position_risk(symbol, stop_loss, take_profit)
            except Exception as e:
                logger.error("DB set_position_risk hatası: %s", e)
            try:
                self.db.set_position_signal_meta(
                    symbol,
                    strategy_name=trade_audit_fields.get("strategy_name"),
                    entry_reason=trade_audit_fields.get("entry_reason"),
                    regime=trade_audit_fields.get("regime"),
                    atr_pct=trade_audit_fields.get("atr_pct"),
                    dir_1h=trade_audit_fields.get("dir_1h"),
                )
            except Exception as e:
                logger.error("DB set_position_signal_meta hatası: %s", e)

        try:
            if self.notifier and getattr(self.notifier, "enabled", False):
                # PnL% hesapla (SELL için)
                _pnl_pct = None
                if signal == "SELL" and realized_pnl is not None and position_before_sell is not None:
                    _entry = float(position_before_sell.get("entry_price") or 0)
                    _cost  = float(position_before_sell.get("cost") or 0)
                    if _cost > 0:
                        _pnl_pct = (float(realized_pnl) / _cost) * 100
                    elif _entry > 0 and exec_amount > 0:
                        _pnl_pct = ((exec_price - _entry) / _entry) * 100

                # Strateji ve regime bilgisi
                _strat = None
                _regime = None
                _reason_tag = str(reason or "")[:40] if reason else None
                if signal_details:
                    _strat  = str(signal_details.get("strategy_name") or "")[:20] or None
                    _regime = str(signal_details.get("regime") or "")[:15] or None
                elif position_before_sell:
                    _strat  = str(position_before_sell.get("strategy_name") or "")[:20] or None
                    _regime = str(position_before_sell.get("regime") or "")[:15] or None

                self.notifier.notify_trade_alert(
                    symbol=symbol,
                    side=signal,
                    amount=exec_amount,
                    price=exec_price,
                    pnl=realized_pnl if signal == "SELL" else None,
                    pnl_pct=_pnl_pct,
                    is_test=self.test_mode,
                    strategy_name=_strat,
                    regime=_regime,
                    reason=_reason_tag,
                )
        except Exception as e:
            logger.error("Notifier hatası: %s", e)

        return order