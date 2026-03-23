# File: backend/main.py (CURRENT WORKING VERSION - PRE-HFT)
from __future__ import annotations

import asyncio
from collections import Counter, deque
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, Tuple
from zoneinfo import ZoneInfo
from backend.re_entry_guard import ReEntryGuard
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from .app_wiring.correlation_wiring import build_correlation_service
from .app_wiring.trend_bias_wiring import build_trend_bias_service
from .binance_service import BinanceService
from .coin_profiles import default_profile, derive_profile_from_research
from .config import BACKEND_DIR, DB_PATH, settings
from .domain.models import Sleeve
from .core.correlation import correlation_penalty
from .cryptopanic_service import CryptoPanicService
from .database import Database
from .middleware.error_handler import error_handler_middleware, register_exception_handlers
from .notifier import TelegramNotifier
from .research import ResearchEngine
from .research_store import ResearchStore
from .risk.correlation_guard import CorrelationGuard
from .risk.execution_health import ExecutionHealthScaler
from .risk.portfolio_heat import PortfolioHeatManager
from .risk_limits import (
    compute_open_positions_unrealized,
    compute_paper_equity_snapshot,
    resolve_daily_loss_limit_usdt,
)
from .strategy import TradingStrategy
from .strategy.long_trend_hold import LongTrendHoldStrategy
from .strategy.medium_continuation import MediumContinuationStrategy
from .strategy.regime_guard import RegimeGuard
from .strategy.router import UNIVERSE_TO_SLEEVE
from .strategy.short_snapback import ShortSnapbackStrategy
from .strategy_snapback import SnapbackStrategy
from .trader import Trader, TradingPausedError
from .universe import get_universe_symbols
from .universe_selector import UniverseConfig, UniverseSelector, compute_next_rebuild_at
from .utils import get_dynamic_coins
from .utils_symbols import normalize_symbol

# ------------------------
# LOGGING
# ------------------------
logger = logging.getLogger(__name__)


# Universe selection defaults (Binance-only)
UNIVERSE_DYNAMIC_N = int(os.getenv("UNIVERSE_DYNAMIC_N", "6") or "6")
UNIVERSE_MIN_VOL_USD = float(os.getenv("UNIVERSE_MIN_VOL_USD", "50000000") or "50000000")
UNIVERSE_MAX_SPREAD_PCT = float(os.getenv("UNIVERSE_MAX_SPREAD_PCT", "0.25") or "0.25")
UNIVERSE_REBUILD_DAYS = int(os.getenv("UNIVERSE_REBUILD_DAYS", "14") or "14")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    datefmt="%H:%M:%S",
)

load_dotenv(BACKEND_DIR / ".env")

# ------------------------
# SETTINGS / GLOBALS
# ------------------------
IS_TEST_MODE = settings.TEST_MODE

TRAIL_PCT = settings.TRAIL_PCT
DEFAULT_SL_PCT = settings.DEFAULT_SL_PCT
DEFAULT_TP_PCT = settings.DEFAULT_TP_PCT

MAX_OPEN_POSITIONS = settings.MAX_OPEN_POSITIONS
COOLDOWN_SECONDS = settings.COOLDOWN_SECONDS
SCAN_TIMEFRAME = os.getenv("SCAN_TIMEFRAME", "15m").strip() or "15m"

trend_bias_svc = None  # type: ignore
correlation_svc = None  # type: ignore

binance_svc: BinanceService | None = None
sentiment_svc: CryptoPanicService | None = None
strategy: TradingStrategy | None = None
snapback_strategy: SnapbackStrategy | None = None
trader_bot: Trader | None = None
db: Database | None = None
notifier: TelegramNotifier | None = None

watchdog_task: asyncio.Task | None = None
scanner_task: asyncio.Task | None = None
universe_task: asyncio.Task | None = None

research_store: ResearchStore | None = None
research_engine: ResearchEngine | None = None

_last_trade_ts: float = 0.0
TR_TZ = ZoneInfo("Europe/Istanbul")
_LAST_DAILY_LOSS_TRIGGER_DAY_TR: str | None = None

regime_guard = RegimeGuard()
heat_mgr = PortfolioHeatManager()
corr_guard = CorrelationGuard()
exec_scaler = ExecutionHealthScaler()
re_entry_guard = ReEntryGuard()
short_strategy = ShortSnapbackStrategy()
medium_strategy = MediumContinuationStrategy()
long_strategy = LongTrendHoldStrategy()

# Scanner regime telemetry (best-effort, in-memory)
_REGIME_HISTORY: deque[str] = deque(maxlen=int(os.getenv("REGIME_HISTORY_MAXLEN", "400")))


def select_strategy(symbol: str):
    sleeve = UNIVERSE_TO_SLEEVE[symbol]
    if sleeve.value == "short":
        return short_strategy, sleeve
    if sleeve.value == "medium":
        return medium_strategy, sleeve
    return long_strategy, sleeve


def can_open_position(symbol: str, sleeve: Sleeve, signal_data, runtime_state) -> tuple[bool, str | None]:
    regime = regime_guard.detect(runtime_state["market_state"])
    if settings.regime_guard_enabled and not regime_guard.is_allowed(sleeve, regime):
        return False, f"regime_block({regime.value})"

    allowed, reason = heat_mgr.can_allocate(
        sleeve=sleeve,
        requested_risk_pct=runtime_state["requested_risk_pct"],
        current_total_heat_pct=runtime_state["current_total_heat_pct"],
        current_sleeve_heat_pct=runtime_state["current_sleeve_heat_pct"],
    )
    if not allowed:
        return False, reason

    if settings.corr_kill_enabled:
        corr_ok, corr_reason = corr_guard.evaluate(
            symbol=symbol,
            corr_to_open_cluster=runtime_state["corr_to_open_cluster"],
            now=datetime.utcnow(),
        )
        if not corr_ok:
            return False, corr_reason

    if runtime_state["position_already_open"]:
        return False, "position_already_open"

    if runtime_state["duplicate_signal"]:
        return False, "duplicate_signal"

    if runtime_state["cooldown_active"]:
        return False, runtime_state["cooldown_reason"]

    if runtime_state["max_open_positions_hit"]:
        return False, "max_open_positions"

    return True, None

# ------------------------
# PRICE CACHE + BACKOFF (THREAD-SAFE)
# ------------------------
_PRICE_LOCK = threading.RLock()

_PRICE_LAST: dict[str, float] = {}
_PRICE_LAST_TS: dict[str, float] = {}
_PRICE_FAIL_COUNT: dict[str, int] = {}
_PRICE_NEXT_ALLOWED_TS: dict[str, float] = {}

_PRICE_CACHE_TTL_S = float(os.getenv("PRICE_CACHE_TTL_S", "2.5"))
_PRICE_FAIL_LIMIT = int(os.getenv("PRICE_FAIL_LIMIT", "8"))
_PRICE_BACKOFF_BASE_S = float(os.getenv("PRICE_BACKOFF_BASE_S", "0.75"))
_PRICE_BACKOFF_CAP_S = float(os.getenv("PRICE_BACKOFF_CAP_S", "20.0"))

# Watchdog per-tick price cache
_WATCHDOG_TICK_PRICE_CACHE: dict[str, float] = {}

# Blocked RAPOR rate-limit (per symbol+reason)
_LAST_BLOCKED_REPORT_TS: dict[Tuple[str, str], float] = {}
_BLOCKED_REPORT_TTL_S = float(os.getenv("BLOCKED_REPORT_TTL_S", "600"))  # default 10 min


def _is_buy_side_decision(value: object) -> bool:
    text = str(value or "").strip().upper()
    return text == "BUY" or text.startswith("BUY(")


def _backoff_delay_s(fail_count: int) -> float:
    fc = max(1, int(fail_count))
    delay = _PRICE_BACKOFF_BASE_S * (2 ** (fc - 1))
    return float(min(_PRICE_BACKOFF_CAP_S, delay))


def _try_create_task(coro) -> None:
    """
    Safe scheduling from sync code: only create_task if a running loop exists.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    try:
        loop.create_task(coro)
    except Exception:
        return


async def _notify_paused_async(source: str, exc: BaseException) -> None:
    if notifier and getattr(notifier, "enabled", False):
        try:
            await notifier.send_paused_alert(source, str(exc)[:300])
        except Exception as notify_err:
            logger.warning("Paused alert send failed: %s", str(notify_err)[:200])


async def run_watchdog_loop_safely() -> None:
    try:
        await watchdog_loop()
    except TradingPausedError as exc:
        logger.info("Watchdog loop paused; disabled state is active. detail=%s", str(exc)[:200])
    except Exception as exc:
        logger.exception("Watchdog loop crashed unexpectedly.")
        try:
            if db:
                db.set_bot_enabled(False)
        except Exception:
            logger.exception("Failed to disable bot after watchdog crash")
        await _notify_paused_async("WATCHDOG_LOOP_CRASHED", exc)
        raise


async def run_scanner_loop_safely() -> None:
    try:
        await scanner_loop()
    except TradingPausedError as exc:
        logger.info("Scanner loop paused; disabled state is active. detail=%s", str(exc)[:200])
    except Exception as exc:
        logger.exception("Scanner loop crashed unexpectedly.")
        try:
            if db:
                db.set_bot_enabled(False)
        except Exception:
            logger.exception("Failed to disable bot after scanner crash")
        await _notify_paused_async("SCANNER_LOOP_CRASHED", exc)
        raise


async def run_universe_rebuild_loop_safely() -> None:
    try:
        await universe_rebuild_loop()
    except Exception as exc:
        # Universe maintenance failure must NOT take the bot down.
        logger.exception("Universe rebuild loop crashed unexpectedly.")
        await _notify_paused_async("UNIVERSE_LOOP_CRASHED", exc)
        # Do not disable bot; keep current universe.
        return


def init_services() -> None:
    global binance_svc, sentiment_svc, strategy, snapback_strategy, trader_bot, db, notifier
    global research_store, research_engine
    global trend_bias_svc, correlation_svc

    db = Database(DB_PATH)
    db.connect()
    repo_root_db_path = (BACKEND_DIR.parent / "trading_bot.db").resolve()
    if repo_root_db_path != db.db_path and repo_root_db_path.exists():
        logger.warning("DB split-brain risk: active_db=%s other_db=%s", db.db_path, repo_root_db_path)
    try:
        open_positions_on_start = len(db.list_open_positions())
    except Exception:
        open_positions_on_start = -1
    logger.info("startup_state db_path=%s open_positions_on_start=%s", db.db_path, open_positions_on_start)
    if IS_TEST_MODE and open_positions_on_start > 0:
        logger.warning(
            "startup_state stale open positions detected in V1 test mode: count=%s; "
            "logging only, no auto-pause/disable here. Runtime pause decisions remain in trader/risk checks.",
            open_positions_on_start,
        )
    notifier = TelegramNotifier()
    def _telegram_critical_hook(reason: str):
        try:
            logger.error("🛑 TELEGRAM CRITICAL: %s", reason)
            if db:
                db.set_bot_enabled(False)
                logger.error("🛑 BOT AUTO-PAUSED (telegram failure). is_enabled=False")
        except Exception as e:
            logger.error("Telegram critical hook failed: %s", str(e)[:200])

    notifier.on_critical = _telegram_critical_hook

    binance_svc = BinanceService()
    sentiment_svc = CryptoPanicService()
    strategy = TradingStrategy()
    snapback_strategy = SnapbackStrategy()
    try:
        logger.info("strategy_config %s", strategy.get_config_snapshot())
    except Exception:
        logger.warning("strategy_config unavailable at startup")

    trend_bias_svc = build_trend_bias_service(binance_svc=binance_svc, logger=logger)
    logger.info("✅ 1H TrendBiasService hazır (TTL cache aktif).")

    env_risk = os.getenv("RISK_PER_TRADE")
    risk_per_trade = float(env_risk) if env_risk is not None else float(settings.RISK_PER_TRADE)

    trader_bot = Trader(
        exchange=binance_svc.exchange,
        db=db,
        notifier=notifier,
        test_mode=IS_TEST_MODE,
        risk_per_trade=risk_per_trade,
    )

    # Research DB is separate to avoid SQLite locking with trading_bot.db
    research_db_path = os.getenv("RESEARCH_DB_PATH", "research.db").strip() or "research.db"
    research_store = ResearchStore(db_path=research_db_path)
    research_engine = ResearchEngine(
        store=research_store,
        fetch_ohlcv=lambda sym, tf, since, limit: binance_svc.exchange.fetch_ohlcv(
            sym, timeframe=tf, since=since, limit=limit
        ),
        sleep_s=float(os.getenv("RESEARCH_INGEST_SLEEP_S", "0.20")),
    )
    research_engine.init_schema()

    try:
        correlation_svc = build_correlation_service(research_store=research_store, logger=logger)
        logger.info("✅ CorrelationService hazır (research.db, TTL cache).")
    except Exception as e:
        correlation_svc = None
        logger.warning("⚠️ CorrelationService init failed: %s", e)


def _spot_has_open_position(symbol: str) -> bool:
    """Spot-safe: SELL sadece elde pozisyon varsa allowed."""
    if not db:
        return False
    try:
        pos = db.get_open_position(normalize_symbol(symbol))
        return bool(pos) and float(pos.get("amount", 0.0) or 0.0) > 0.0
    except Exception:
        return False


async def watchdog_loop() -> None:
    """
    High-priority risk loop.
    Runs frequently to enforce daily loss limits and trailing-stop/SL/TP logic.
    Must NOT depend on scanner timing.
    """
    logger.info("--- 🛡️ WATCHDOG LOOP AKTİF (5s) ---")

    tick = 0
    while True:
        try:
            if db:
                try:
                    check_daily_loss_and_kill_switch()
                except TradingPausedError:
                    raise
                except Exception:
                    pass

                try:
                    risk_watchdog_manage_positions()
                except TradingPausedError:
                    raise
                except Exception:
                    pass

                tick += 1
                if tick % 12 == 0:
                    try:
                        open_cnt = len(db.get_open_positions())
                    except Exception:
                        open_cnt = -1
                    logger.info("watchdog_tick open_positions=%s", open_cnt)

        except TradingPausedError as exc:
            logger.info("Watchdog loop paused; disabled state is active. detail=%s", str(exc)[:200])
        except Exception as e:
            logger.error("Watchdog loop error: %s", e)
            if notifier:
                try:
                    await notifier.send_error("Watchdog Loop", str(e))
                except Exception:
                    pass

        await asyncio.sleep(5)


async def scanner_loop() -> None:
    """
    Opportunity scanning loop.
    Runs on a slower cadence (COOLDOWN_SECONDS) to evaluate signals and place trades.

    IMPORTANT:
      - Telemetry must NOT stop when max positions is full.
      - We keep scanning and reporting, but block execution via execute_trade=False.
    """
    global _last_trade_ts, _BLOCKED_REPORT_TTL_S

    logger.info("--- 🔎 SCANNER LOOP AKTİF ---")
    await asyncio.sleep(5)

    round_no = 0

    scan_interval = int(COOLDOWN_SECONDS) if int(COOLDOWN_SECONDS) > 0 else 300

    telemetry_interval_when_blocked = int(os.getenv("TELEMETRY_INTERVAL_BLOCKED_SECONDS", "60") or "60")
    telemetry_interval_when_blocked = max(15, min(300, telemetry_interval_when_blocked))

    # clamp blocked-report TTL
    try:
        _BLOCKED_REPORT_TTL_S = float(os.getenv("BLOCKED_REPORT_TTL_S", str(_BLOCKED_REPORT_TTL_S)))
    except Exception:
        pass
    _BLOCKED_REPORT_TTL_S = float(max(30.0, min(3600.0, _BLOCKED_REPORT_TTL_S)))

    while True:
        round_no += 1
        effective_sleep_after_scan = scan_interval

        try:
            if not db:
                await asyncio.sleep(5)
                continue

            state = db.get_bot_state()
            is_enabled = bool(state.get("is_enabled", True))
            risk_multiplier = float(state.get("risk_multiplier", 1.0))

            if not (is_enabled and risk_multiplier > 0):
                logger.info(
                    "STOP MODE: new trades paused (is_enabled=%s, risk_multiplier=%s). Sleeping 30s.",
                    is_enabled,
                    risk_multiplier,
                )
                await asyncio.sleep(30)
                continue

            now = time.time()
            if COOLDOWN_SECONDS > 0 and (now - _last_trade_ts) < COOLDOWN_SECONDS:
                wait = int(COOLDOWN_SECONDS - (now - _last_trade_ts))
                logger.info("⏳ Trade cooldown aktif (%ss). 15s bekleniyor...", wait)
                await asyncio.sleep(15)
                continue

            # scanner-level trade-only gating
            trading_allowed = True
            scanner_blocked_reason: str | None = None
            try:
                open_pos = db.get_open_positions()
                if len(open_pos) >= MAX_OPEN_POSITIONS:
                    trading_allowed = False
                    scanner_blocked_reason = f"max_open_positions({len(open_pos)}/{MAX_OPEN_POSITIONS})"
                    logger.info(
                        "👀 Max open positions dolu. TARAMA DEVAM, TRADE BLOKLU: %s",
                        scanner_blocked_reason,
                    )
            except Exception:
                trading_allowed = False
                scanner_blocked_reason = "positions_check_failed"

            effective_sleep_after_scan = (
                scan_interval if trading_allowed else min(scan_interval, telemetry_interval_when_blocked)
            )

            # Trading universe is persisted in DB and mirrored into UNIVERSE_SYMBOLS env.
            # We scan the full active universe (anchors + dynamic picks).
            active_coins = get_universe_symbols()
            logger.info("🎯 Taranan Coinler: %s", active_coins)

            holds = buys = sells = 0
            top_candidates: list[dict] = []

            for symbol in active_coins:
                try:
                    res = await run_analysis(
                        symbol,
                        timeframe=SCAN_TIMEFRAME,
                        execute_trade=bool(trading_allowed),
                        risk_multiplier=risk_multiplier,
                    )

                    decision = res.get("decision", "HOLD")
                    details = res.get("details") or {}
                    score = float(details.get("score", 0) or 0.0)
                    reason = str(details.get("reason", ""))

                    regime = details.get("regime", "UNKNOWN")
                    er = details.get("er", None)
                    adx = details.get("adx", None)
                    vol_ratio = details.get("vol_ratio", None)
                    strategy_name = str(details.get("strategy_name") or "breakout")
                    gate_status = dict(details.get("gate_status") or {})
                    hold_fail_reasons = list(details.get("hold_fail_reasons") or [])

                    try:
                        _REGIME_HISTORY.append(str(regime or "UNKNOWN").upper())
                    except Exception:
                        pass

                    effective_thresholds = dict(details.get("effective_thresholds") or {})
                    buy_th = effective_thresholds.get("buy")
                    sell_th = effective_thresholds.get("sell")

                    gap = None
                    try:
                        if buy_th is not None:
                            gap = float(buy_th) - float(score)
                    except Exception:
                        gap = None

                    corr_factor = res.get("corr_factor", None)
                    corr_reason = res.get("corr_reason", None)

                    trade_blocked = (not trading_allowed)
                    execution_block_reason = res.get("blocked_reason")
                    execution_blocked = bool(execution_block_reason)

                    if decision == "BUY" and trading_allowed and not execution_blocked:
                        buys += 1
                    elif decision == "SELL" and trading_allowed and not execution_blocked:
                        sells += 1
                    else:
                        holds += 1

                    effective_decision = decision
                    if trade_blocked and decision in ("BUY", "SELL"):
                        effective_decision = f"{decision}(blocked_scanner)"
                    elif (not trade_blocked) and execution_blocked and decision in ("BUY", "SELL"):
                        if "blocked" not in str(decision).lower():
                            effective_decision = f"{decision}(blocked_exec)"

                    combined_blocked_reason = str(scanner_blocked_reason) if trade_blocked else (
                        str(execution_block_reason) if execution_block_reason else None
                    )

                    should_audit_buy_signal = _is_buy_side_decision(decision) or _is_buy_side_decision(effective_decision)
                    audit_signal = "BUY" if should_audit_buy_signal else str(decision)

                    if should_audit_buy_signal and db is not None:
                        try:
                            db.add_signal_audit(
                                symbol=symbol,
                                strategy_name=strategy_name,
                                decision=effective_decision,
                                signal=audit_signal,
                                score=float(score),
                                buy_threshold=None if buy_th is None else float(buy_th),
                                gap_to_threshold=None if gap is None else float(gap),
                                regime=None if regime is None else str(regime),
                                dir_1h=(None if details.get("dir_1h") is None else str(details.get("dir_1h"))),
                                atr_pct=details.get("atr_pct"),
                                price=None if res.get("price") is None else float(res.get("price") or 0.0),
                                blocked_reason=combined_blocked_reason,
                                trade_blocked=bool(trade_blocked),
                                exec_blocked=bool(execution_blocked),
                                gate_status=gate_status,
                                hold_fail_reasons=hold_fail_reasons,
                                reason=reason,
                                risk_multiplier=float(risk_multiplier),
                                corr_factor=None if corr_factor is None else float(corr_factor),
                                corr_reason=(None if corr_reason is None else str(corr_reason)),
                            )
                            logger.info(
                                "signal_audit_write symbol=%s decision=%s strategy=%s blocked_reason=%s ok=true",
                                symbol,
                                effective_decision,
                                strategy_name,
                                combined_blocked_reason,
                            )
                        except Exception as audit_e:
                            logger.error(
                                "signal_audit_write symbol=%s decision=%s strategy=%s blocked_reason=%s ok=false err=%s",
                                symbol,
                                effective_decision,
                                strategy_name,
                                combined_blocked_reason,
                                str(audit_e)[:300],
                            )

                    # RAPOR logging policy
                    if decision in ("BUY", "SELL"):
                        if trading_allowed and not execution_blocked:
                            logger.info("📢 RAPOR: %s", res)
                        else:
                            br = str(scanner_blocked_reason) if trade_blocked else str(execution_block_reason or "blocked")
                            key = (normalize_symbol(symbol), br)
                            last_ts = float(_LAST_BLOCKED_REPORT_TS.get(key, 0.0))
                            if (now - last_ts) >= _BLOCKED_REPORT_TTL_S:
                                _LAST_BLOCKED_REPORT_TS[key] = now
                                logger.info("📢 RAPOR(blocked): %s", res)

                    top_candidates.append(
                        {
                            "symbol": symbol,
                            "strategy_name": strategy_name,
                            "decision": effective_decision,
                            "score": score,
                            "reason": reason[:220],
                            "regime": regime,
                            "er": None if er is None else float(er),
                            "adx": None if adx is None else float(adx),
                            "vol_ratio": None if vol_ratio is None else float(vol_ratio),
                            "effective_thresholds": effective_thresholds,
                            "buy_th": None if buy_th is None else float(buy_th),
                            "sell_th": None if sell_th is None else float(sell_th),
                            "gap": gap,
                            "corr": None if corr_factor is None else float(corr_factor),
                            "corr_reason": corr_reason,
                            "trade_blocked": trade_blocked,
                            "scanner_blocked_reason": scanner_blocked_reason,
                            "execution_blocked": execution_blocked,
                            "execution_block_reason": execution_block_reason,
                            "gate_status": gate_status,
                            "hold_fail_reasons": hold_fail_reasons,
                        }
                    )

                    await asyncio.sleep(1.5)

                except TradingPausedError:
                    raise
                except Exception as inner_e:
                    logger.error("Scanner döngü hatası (%s): %s", symbol, inner_e)
                    continue

            top_candidates.sort(key=lambda x: (0 if str(x["decision"]) != "HOLD" else 1, -abs(float(x["score"]))))

            logger.info(
                "🧾 ROUND#%s summary: BUY=%s SELL=%s HOLD=%s | risk_multiplier=%.2f | trade_blocked=%s reason=%s",
                round_no,
                buys,
                sells,
                holds,
                risk_multiplier,
                (not trading_allowed),
                scanner_blocked_reason,
            )

            for c in top_candidates[:2]:
                logger.info(
                    "🔎 candidate: %s decision=%s score=%.2f buy_th=%s gap=%s corr=%s regime=%s adx=%s er=%s volx=%s "
                    "strategy=%s trade_blocked=%s scanner_reason=%s exec_blocked=%s exec_reason=%s "
                    "gate_status=%s hold_fails=%s reason=%s",
                    c["symbol"],
                    c["decision"],
                    c["score"],
                    (c.get("effective_thresholds") or {}).get("buy"),
                    c.get("gap"),
                    c.get("corr_reason") or c.get("corr"),
                    c["regime"],
                    c["adx"],
                    c["er"],
                    c["vol_ratio"],
                    c["strategy_name"],
                    c["trade_blocked"],
                    c.get("scanner_blocked_reason"),
                    c["execution_blocked"],
                    c.get("execution_block_reason"),
                    c.get("gate_status"),
                    c.get("hold_fail_reasons"),
                    c["reason"],
                )

        except TradingPausedError as exc:
            logger.info("Scanner loop paused; disabled state is active. detail=%s", str(exc)[:200])
            effective_sleep_after_scan = max(15, effective_sleep_after_scan)
        except Exception as e:
            logger.error("Kritik Scanner Hatası: %s", e)
            if notifier:
                try:
                    await notifier.send_error("Scanner Loop", str(e))
                except Exception:
                    pass

        await asyncio.sleep(effective_sleep_after_scan)


async def _rebuild_universe(reason: str) -> dict:
    """Rebuild active universe (Binance-only).

    - Does NOT close positions.
    - Updates DB + mirrors into UNIVERSE_SYMBOLS env.
    """
    if not (db and binance_svc and getattr(binance_svc, "exchange", None)):
        raise RuntimeError("services_not_ready")

    cfg = UniverseConfig(
        anchors=("BTC/USDT", "ETH/USDT"),
        dynamic_n=int(UNIVERSE_DYNAMIC_N),
        min_quote_volume_usd=float(UNIVERSE_MIN_VOL_USD),
        max_spread_pct=float(UNIVERSE_MAX_SPREAD_PCT),
    )

    selector = UniverseSelector(exchange=binance_svc.exchange, cfg=cfg)

    # run sync ccxt calls off the event loop
    pick = await asyncio.to_thread(selector.rebuild)

    next_at = compute_next_rebuild_at(days=int(UNIVERSE_REBUILD_DAYS))
    db.set_universe_state(pick.symbols, next_rebuild_at=next_at, reason=reason)

    # mirror to env for existing call sites
    os.environ["UNIVERSE_SYMBOLS"] = ",".join(pick.symbols)

    return {
        "ok": True,
        "symbols": pick.symbols,
        "next_rebuild_at": next_at,
        "generated_at": pick.generated_at,
        "cfg": pick.cfg,
        # Ranked list can be large; still useful for audits
        "ranked": pick.ranked[:50],
    }


def _get_universe_rebuild_due_state(st: dict | None, now_utc: datetime | None = None) -> tuple[bool, str | None]:
    if st is None:
        return False, compute_next_rebuild_at(days=int(UNIVERSE_REBUILD_DAYS))

    nxt = st.get("next_rebuild_at")
    if not nxt:
        return False, compute_next_rebuild_at(days=int(UNIVERSE_REBUILD_DAYS))

    try:
        dt_nxt = datetime.fromisoformat(str(nxt))
        if dt_nxt.tzinfo is None:
            dt_nxt = dt_nxt.replace(tzinfo=ZoneInfo("UTC"))
        now_dt = now_utc or datetime.now(tz=ZoneInfo("UTC"))
        return now_dt >= dt_nxt, None
    except Exception:
        return True, None


async def universe_rebuild_loop() -> None:
    """Maintenance loop: rebuild universe when due.

    Conservative schedule:
      - check every 6 hours
      - rebuild only if due
    """
    logger.info("universe_loop_started rebuild_days=%s dynamic_n=%s", UNIVERSE_REBUILD_DAYS, UNIVERSE_DYNAMIC_N)
    await asyncio.sleep(5)

    while True:
        try:
            if db:
                st = db.get_universe_state()
                due, init_next = _get_universe_rebuild_due_state(st)

                if init_next:
                    db.set_universe_state(st.get("symbols") or get_universe_symbols(), next_rebuild_at=init_next, reason="INIT")
                    st["next_rebuild_at"] = init_next
                    logger.info("universe_schedule_initialized next_rebuild_at=%s", init_next)

                if due:
                    logger.info("universe_rebuild_due next_rebuild_at=%s", st.get("next_rebuild_at"))
                    try:
                        rep = await _rebuild_universe(reason="AUTO_SCHEDULE")
                        logger.info("universe_rebuilt symbols=%s", rep.get("symbols"))
                        if notifier and getattr(notifier, "enabled", False):
                            try:
                                await notifier.send_info("Universe rebuilt", f"{rep.get('symbols')}")
                            except Exception:
                                pass
                    except Exception as e:
                        logger.warning("universe_rebuild_failed err=%s", str(e)[:240])
        except Exception:
            logger.exception("universe_loop_tick_failed")

        await asyncio.sleep(6 * 60 * 60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global watchdog_task, scanner_task, universe_task

    init_services()
    assert db and notifier

    # Load persisted universe into process env (keeps existing call sites unchanged)
    try:
        u = db.get_universe_state()
        os.environ["UNIVERSE_SYMBOLS"] = u.get("symbols_csv") or os.environ.get("UNIVERSE_SYMBOLS", "")
        logger.info("universe_loaded symbols=%s updated_at=%s", u.get("symbols"), u.get("updated_at"))
    except Exception as e:
        logger.warning("universe_load_failed err=%s", str(e)[:200])

    forced_universe = (os.getenv("V1_FORCE_UNIVERSE_SYMBOLS") or "").strip()
    if forced_universe:
        os.environ["UNIVERSE_SYMBOLS"] = forced_universe
        logger.warning("V1 force universe aktif: %s", forced_universe)

    await notifier.start()

    watchdog_task = asyncio.create_task(run_watchdog_loop_safely(), name="watchdog_loop")
    scanner_task = asyncio.create_task(run_scanner_loop_safely(), name="scanner_loop")
    forced_universe = (os.getenv("V1_FORCE_UNIVERSE_SYMBOLS") or "").strip()

    if forced_universe:
        universe_task = None
        logger.warning("Universe rebuild loop pasif: V1 force universe aktif.")
    else:
        universe_task = asyncio.create_task(run_universe_rebuild_loop_safely(), name="universe_rebuild_loop")

    try:
        yield
    finally:
        for t in (watchdog_task, scanner_task, universe_task):
            if t:
                t.cancel()
        for t in (watchdog_task, scanner_task, universe_task):
            if t:
                try:
                    await t
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

        try:
            await notifier.stop()
        except Exception:
            pass

        try:
            if binance_svc and getattr(binance_svc, "exchange", None):
                try:
                    binance_svc.exchange.close()
                except Exception:
                    pass
        except Exception:
            pass

        try:
            if db:
                db.close_db()
        except Exception:
            pass


app = FastAPI(title="AI Crypto Trading Bot", lifespan=lifespan)
register_exception_handlers(app)
app.middleware("http")(error_handler_middleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _safe_price(sym: str) -> Optional[float]:
    """
    Safe price getter with:
      - TTL cache (fast path)
      - per-symbol backoff on failures
      - hard failure limit (optional hook point)
    """
    global binance_svc, notifier, db

    if not sym:
        return None

    now = time.time()

    # 1) TTL cache fast path
    try:
        with _PRICE_LOCK:
            last_ts = float(_PRICE_LAST_TS.get(sym, 0.0))
            if (now - last_ts) <= _PRICE_CACHE_TTL_S:
                last_px = _PRICE_LAST.get(sym)
                if last_px is not None and last_px > 0:
                    return float(last_px)
    except Exception:
        pass

    # 2) backoff gate
    try:
        with _PRICE_LOCK:
            next_ok = float(_PRICE_NEXT_ALLOWED_TS.get(sym, 0.0))
            if now < next_ok:
                last_px = _PRICE_LAST.get(sym)
                return float(last_px) if (last_px is not None and last_px > 0) else None
    except Exception:
        pass

    # 3) remote fetch
    px: Optional[float] = None
    try:
        if not binance_svc:
            return None
        px = float(binance_svc.get_ticker_price(sym) or 0.0)
        if px <= 0:
            raise RuntimeError("empty_price")
    except Exception as e:
        try:
            with _PRICE_LOCK:
                fc = int(_PRICE_FAIL_COUNT.get(sym, 0)) + 1
                _PRICE_FAIL_COUNT[sym] = fc
                delay = _backoff_delay_s(fc)
                _PRICE_NEXT_ALLOWED_TS[sym] = now + delay

            logger.warning(
                "price_fetch_failed sym=%s fail=%s backoff=%.1fs err=%s",
                sym,
                fc,
                delay,
                str(e)[:200],
            )

            if fc >= _PRICE_FAIL_LIMIT:
                try:
                    logger.error("🛑 PRICE FEED UNSTABLE sym=%s fail=%s => bot auto-pause", sym, fc)
                    if db:
                        db.set_bot_enabled(False)
                    if notifier and getattr(notifier, "enabled", False):
                        _try_create_task(notifier.send_error("PriceFeed", f"{sym} price failures={fc}"))
                except Exception:
                    pass
        except Exception:
            pass

        try:
            with _PRICE_LOCK:
                last_px = _PRICE_LAST.get(sym)
            return float(last_px) if (last_px is not None and last_px > 0) else None
        except Exception:
            return None

    # 4) success: update caches + reset failures
    try:
        with _PRICE_LOCK:
            _PRICE_LAST[sym] = float(px)
            _PRICE_LAST_TS[sym] = now
            _PRICE_FAIL_COUNT[sym] = 0
            _PRICE_NEXT_ALLOWED_TS[sym] = 0.0
    except Exception:
        pass

    return float(px)


def _paper_equity_usdt() -> float:
    if not db:
        return 0.0
    snap = compute_paper_equity_snapshot(db=db, price_fn=_safe_price)
    return float(snap.equity_usdt)


def _resolve_profile(symbol: str) -> dict:
    sym = normalize_symbol(symbol)
    prof = None
    try:
        if db:
            prof = db.get_coin_profile(sym)
    except Exception:
        prof = None

    if isinstance(prof, dict) and prof:
        prof["symbol"] = sym
        return prof

    try:
        p = default_profile(sym).as_dict()
        p["symbol"] = sym
        return p
    except Exception:
        return {
            "symbol": sym,
            "buy_threshold": 3.0,
            "sell_threshold": -3.0,
            "min_volume_ratio": 0.50,
            "min_atr_pct": 0.002,
            "max_atr_pct": 0.060,
            "downtrend_buy_penalty": 2.0,
            "uptrend_buy_boost": 1.0,
            "risk_mult": 1.0,
        }


def check_daily_loss_and_kill_switch() -> bool:
    global _LAST_DAILY_LOSS_TRIGGER_DAY_TR

    if not db:
        return False

    today_tr = datetime.now(tz=TR_TZ).date().isoformat()
    if _LAST_DAILY_LOSS_TRIGGER_DAY_TR and _LAST_DAILY_LOSS_TRIGGER_DAY_TR != today_tr:
        logger.info(
            "daily_loss_guard reset_for_new_tr_day prev_day=%s new_day=%s auto_reenable=%s",
            _LAST_DAILY_LOSS_TRIGGER_DAY_TR,
            today_tr,
            False,
        )
        _LAST_DAILY_LOSS_TRIGGER_DAY_TR = None

    equity = _paper_equity_usdt() if IS_TEST_MODE else 0.0
    limit_usdt = resolve_daily_loss_limit_usdt(equity_usdt=equity)
    if limit_usdt <= 0:
        return False

    try:
        realized = float(db.get_today_realized_pnl())
    except Exception:
        realized = 0.0

    try:
        unrealized = float(compute_open_positions_unrealized(db=db, price_fn=_safe_price))
    except Exception:
        unrealized = 0.0

    total = float(realized) + float(unrealized)

    if total <= -abs(limit_usdt):
        already_triggered_today = _LAST_DAILY_LOSS_TRIGGER_DAY_TR == today_tr
        try:
            db.set_bot_enabled(False)
        except Exception:
            pass

        if already_triggered_today:
            # Idempotent: already paused today; avoid spam.
            return True

        _LAST_DAILY_LOSS_TRIGGER_DAY_TR = today_tr

        msg = (
            f"DAILY_LOSS_LIMIT: PnL={total:.2f} <= -{abs(limit_usdt):.2f} USDT "
            f"(realized={realized:.2f}, unrealized={unrealized:.2f}, equity≈{equity:.2f}, "
            f"pct={float(os.getenv('DAILY_MAX_LOSS_PCT','0.02')):.4f})"
        )
        logger.error(msg)

        # Notify (sync-safe wrapper)
        try:
            if notifier and getattr(notifier, "enabled", False):
                notifier.notify_paused_alert(
                    reason="DAILY_LOSS_LIMIT",
                    detail=msg[:300],
                )
        except Exception as notify_err:
            logger.warning("Paused alert send failed (DAILY_LOSS_LIMIT): %s", str(notify_err)[:200])

        # Fail-closed: surface pause via API middleware.
        raise TradingPausedError(msg)

    return False


def risk_watchdog_manage_positions() -> None:
    """
    Risk watchdog: manages open positions (trail SL, SL/TP exits).
    Fail-closed: if no price, do nothing.
    """
    global _WATCHDOG_TICK_PRICE_CACHE

    if not db or not trader_bot:
        return

    try:
        positions = db.get_open_positions()
    except Exception as e:
        logger.error("watchdog positions fetch failed: %s", str(e)[:200])
        return

    _WATCHDOG_TICK_PRICE_CACHE = {}

    for p in positions:
        try:
            sym = normalize_symbol(p.get("symbol"))
            amt = float(p.get("amount", 0.0) or 0.0)
            if amt <= 0 or not sym:
                continue

            entry = float(p.get("entry_price", 0.0) or 0.0)
            if entry <= 0:
                continue

            sl = p.get("stop_loss")
            tp = p.get("take_profit")
            hp = p.get("highest_price")

            sl = float(entry * (1.0 - DEFAULT_SL_PCT)) if sl is None else float(sl)
            tp = float(entry * (1.0 + DEFAULT_TP_PCT)) if tp is None else float(tp)
            hp = float(entry) if hp is None else float(hp)

            if sym in _WATCHDOG_TICK_PRICE_CACHE:
                px = _WATCHDOG_TICK_PRICE_CACHE[sym]
            else:
                px = _safe_price(sym)
                if px is None or px <= 0:
                    continue
                _WATCHDOG_TICK_PRICE_CACHE[sym] = px

            if px > hp:
                hp = px
                try:
                    db.update_highest_price(sym, hp)
                except Exception:
                    pass

                trail_sl = float(hp) * (1.0 - TRAIL_PCT)
                if trail_sl > sl:
                    sl = trail_sl
                    try:
                        db.set_position_risk(sym, sl, tp)
                    except Exception:
                        pass

            if px <= float(sl):
                trader_bot.execute_trade(sym, "SELL", px, 1.0, reason="STOP_LOSS")
                continue

            if px >= float(tp):
                trader_bot.execute_trade(sym, "SELL", px, 1.0, reason="TAKE_PROFIT")
                continue

        except TradingPausedError:
            raise
        except Exception as e:
            logger.error("watchdog_manage_position_failed sym=%s err=%s", str(p.get("symbol")), str(e)[:200])
            continue


# ------------------------
# RESEARCH ENDPOINTS
# ------------------------
@app.get("/research/universe")
def research_universe():
    return {"symbols": get_universe_symbols()}


# ------------------------
# UNIVERSE (TRADING) ENDPOINTS
# ------------------------
@app.get("/universe/state")
def universe_state():
    if not db:
        raise HTTPException(status_code=500, detail="DB hazır değil")
    st = db.get_universe_state()
    return {
        "ok": True,
        "symbols": st.get("symbols"),
        "updated_at": st.get("updated_at"),
        "next_rebuild_at": st.get("next_rebuild_at"),
        "last_reason": st.get("last_reason"),
        "cfg": {
            "dynamic_n": UNIVERSE_DYNAMIC_N,
            "min_quote_volume_usd": UNIVERSE_MIN_VOL_USD,
            "max_spread_pct": UNIVERSE_MAX_SPREAD_PCT,
            "rebuild_days": UNIVERSE_REBUILD_DAYS,
        },
    }


@app.post("/universe/rebuild")
async def universe_rebuild(reason: str = "MANUAL"):
    try:
        rep = await _rebuild_universe(reason=reason)
        return rep
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"universe_rebuild_failed: {str(e)[:240]}")


@app.post("/research/ingest")
def research_ingest(timeframe: str = "1h", days_back: int = 365, limit: int = 1000):
    if not research_engine:
        raise HTTPException(status_code=500, detail="research_engine not ready")

    symbols = get_universe_symbols()
    results = []
    for s in symbols:
        try:
            res = research_engine.ingest_symbol(symbol=s, timeframe=timeframe, days_back=int(days_back), limit=int(limit))
            results.append(
                {"symbol": res.symbol, "timeframe": res.timeframe, "inserted": res.inserted, "last_ts_ms": res.last_ts_ms}
            )
        except Exception as e:
            results.append({"symbol": normalize_symbol(s), "timeframe": timeframe, "error": str(e)})

    return {
        "ok": True,
        "db": os.getenv("RESEARCH_DB_PATH", "research.db"),
        "timeframe": timeframe,
        "days_back": int(days_back),
        "results": results,
    }


@app.get("/research/report")
def research_report(timeframe: str = "1h", since_days: int = 365):
    if not research_engine:
        raise HTTPException(status_code=500, detail="research_engine not ready")
    symbols = get_universe_symbols()
    return research_engine.analyze_universe(symbols=symbols, timeframe=timeframe, since_days=int(since_days))


@app.get("/health")
def health_check():
    return {
        "status": "active",
        "mode": "test" if IS_TEST_MODE else "live",
        "services": {
            "binance": "connected" if binance_svc else "error",
            "database": "connected" if db else "error",
            "telegram": "active" if notifier and getattr(notifier, "enabled", False) else "disabled",
        },
        "risk": {
            "daily_loss_pct": float(os.getenv("DAILY_MAX_LOSS_PCT", "0.02")),
            "daily_loss_usdt_fixed": float(os.getenv("DAILY_MAX_LOSS_USDT", "0") or 0),
        },
        "research": {"db": os.getenv("RESEARCH_DB_PATH", "research.db")},
    }


@app.get("/profiles")
def list_profiles():
    if not db:
        raise HTTPException(status_code=500, detail="DB hazır değil")
    return {"profiles": db.list_coin_profiles()}


@app.post("/profiles/upsert")
def upsert_profile(payload: dict):
    if not db:
        raise HTTPException(status_code=500, detail="DB hazır değil")
    if "symbol" not in payload:
        raise HTTPException(status_code=400, detail="symbol required")
    payload["symbol"] = normalize_symbol(payload["symbol"])
    db.upsert_coin_profile(payload)
    return {"ok": True, "profile": db.get_coin_profile(payload["symbol"])}


@app.post("/profiles/seed-defaults")
def seed_default_profiles():
    if not db:
        raise HTTPException(status_code=500, detail="DB hazır değil")
    symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT", "BNB/USDT", "TRX/USDT"]
    seeded = []
    for s in symbols:
        p = default_profile(s).as_dict()
        db.upsert_coin_profile(p)
        seeded.append(p)
    return {"ok": True, "seeded": seeded}


@app.post("/profiles/seed-from-research")
def seed_from_research(timeframe: str = "1h", since_days: int = 365):
    if not db:
        raise HTTPException(status_code=500, detail="DB hazır değil")
    try:
        report = research_report(timeframe=timeframe, since_days=since_days)  # type: ignore[name-defined]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"research_report not available: {e}")

    seeded = []
    for r in report.get("reports", []):
        if "error" in r:
            continue
        sym = normalize_symbol(r["symbol"])
        vol_ann = float(r.get("vol_ann", 0.0))
        mdd = float(r.get("max_drawdown", 0.0))
        p = derive_profile_from_research(sym, vol_ann=vol_ann, max_drawdown=mdd).as_dict()
        db.upsert_coin_profile(p)
        seeded.append(p)

    return {"ok": True, "seeded": seeded, "timeframe": timeframe, "since_days": int(since_days)}


@app.get("/paper/balances")
def paper_balances():
    if not db:
        raise HTTPException(status_code=500, detail="DB hazır değil")
    try:
        balances = db.list_paper_balances()
        return {"test_mode": IS_TEST_MODE, "balances": balances, "equity_usdt": _paper_equity_usdt()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"paper_balances okunamadı: {e}")


@app.post("/paper/reset")
def paper_reset(usdt: float = 100.0, clear_positions: bool = True, clear_trades: bool = False):
    if not db:
        raise HTTPException(status_code=500, detail="DB hazır değil")
    try:
        balances = db.reset_paper_balances(usdt=usdt, clear_positions=clear_positions, clear_trades=clear_trades)
        return {"ok": True, "balances": balances, "equity_usdt": _paper_equity_usdt()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"paper_reset başarısız: {e}")


@app.get("/analyze/{symbol:path}")
async def analyze_endpoint(symbol: str, timeframe: str = "15m"):
    symbol_n = normalize_symbol(symbol)
    return await run_analysis(symbol_n, timeframe, execute_trade=False)


@app.get("/dynamic-coins")
def get_current_opportunities():
    # Backwards-compatible alias.
    return get_universe_symbols()


@app.get("/dashboard/positions")
def get_positions_endpoint():
    if not db or not binance_svc:
        return []

    open_positions = db.get_open_positions()
    logger.info("dashboard_positions db_path=%s row_count=%s", db.db_path, len(open_positions))
    result = []

    for p in open_positions:
        symbol = normalize_symbol(p.get("symbol"))
        entry_price = float(p.get("entry_price", 0.0))
        amount = float(p.get("amount", 0.0))

        current_price = float(_safe_price(symbol) or entry_price) if symbol else entry_price
        unrealized_pnl = (current_price - entry_price) * amount

        result.append(
            {
                "symbol": symbol,
                "entry_price": entry_price,
                "entry_amount": amount,
                "entry_time": p.get("opened_at") or p.get("updated_at"),
                "current_price": current_price,
                "unrealized_pnl": round(unrealized_pnl, 2),
                "stop_loss": p.get("stop_loss"),
                "take_profit": p.get("take_profit"),
                "highest_price": p.get("highest_price"),
            }
        )

    return result


@app.get("/dashboard/trades")
def get_trades_endpoint(limit: int = 20):
    if not db:
        return []
    return db.get_dashboard_trades(limit)


@app.get("/dashboard/summary")
def get_summary_endpoint():
    if not db:
        return {}
    return db.get_dashboard_summary()



@app.post("/trade/close/{symbol:path}")
def force_close_endpoint(symbol: str):
    """
    Manual position close from dashboard.
    Now includes re-entry guard tracking!
    """
    # Check services
    if any(x is None for x in (db, trader_bot)):
        raise HTTPException(status_code=500, detail="Servisler hazir degil")

    # Normalize symbol
    symbol_n = normalize_symbol(symbol)
    logger.info("force_close requested symbol=%r normalized=%r", symbol, symbol_n)
    
    # Get open positions for debug
    try:
        open_positions = db.get_open_positions()
        open_symbols = [normalize_symbol(p.get("symbol")) for p in open_positions if p.get("symbol")]
    except Exception:
        open_symbols = []
    
    logger.info(
        "force_close debug db_path=%s requested=%s normalized=%s open_symbols=%s",
        db.db_path,
        symbol,
        symbol_n,
        open_symbols,
    )
    
    # Get position BEFORE closing (needed for re-entry guard!)
    pos = db.get_open_position(symbol_n)
    logger.info("force_close position_found symbol=%s found=%s", symbol_n, bool(pos))
    
    if not pos:
        raise HTTPException(status_code=404, detail="Acik pozisyon bulunamadi")
    
    # Get entry price BEFORE closing
    entry_price = pos.get("entry_price", 0)
    
    # Get current price
    px = _safe_price(symbol_n)
    if px is None:
        raise HTTPException(status_code=500, detail="Anlik fiyat alinamadi")
    
    # Execute the close trade
    result = trader_bot.execute_trade(
        symbol=symbol_n,
        signal="SELL",
        current_price=px,
        risk_multiplier=1.0,
        reason="MANUAL_CLOSE",
    )
    
    # CRITICAL: Mark this sell for re-entry guard!
    if result:
        try:
            # Calculate PnL for logging
            pnl_pct = ((px - entry_price) / entry_price) if entry_price > 0 else 0
            
            # Track in re-entry guard
            re_entry_guard.mark_sell(
                symbol=symbol_n,
                sell_price=px,
                entry_price=entry_price,
                reason="manual_dashboard_close"
            )
            
            # Log with detailed info
            logger.info(
                "🔴 MANUAL CLOSE TRACKED: %s @ $%.2f (entry: $%.2f, PnL: %.2f%%)",
                symbol_n, px, entry_price, pnl_pct * 100
            )
            
        except Exception as e:
            # Don't fail the close if re-entry guard fails
            logger.error("Failed to mark manual close for re-entry guard: %s", e)
        
        return {"status": "closed", "detail": result}
    
    raise HTTPException(status_code=500, detail="Satis islemi basarisiz")


@app.get("/debug/open_positions")
def debug_open_positions():
    if not db:
        raise HTTPException(status_code=500, detail="DB hazir degil")
    return {"rows": db.list_open_positions()}  # yoksa sen yaz: SELECT * FROM positions WHERE status='OPEN'

@app.get("/control/status")
def control_status():
    if not db:
        raise HTTPException(status_code=500, detail="DB hazır değil")
    return db.get_bot_state()


@app.get("/bot/state")
def bot_state(last_n: int = 200):
    """Operational snapshot for dashboards / automation.

    Returns a stable, machine-readable payload.
    """
    if not db:
        raise HTTPException(status_code=500, detail="DB hazır değil")

    st = db.get_bot_state()
    try:
        open_positions_count = len(db.get_open_positions())
    except Exception:
        open_positions_count = -1

    try:
        daily_realized_pnl = float(db.get_today_realized_pnl())
    except Exception:
        daily_realized_pnl = 0.0

    equity = _paper_equity_usdt() if IS_TEST_MODE else None
    limit = resolve_daily_loss_limit_usdt(equity_usdt=float(equity or 0.0)) if IS_TEST_MODE else None

    n = max(1, min(2000, int(last_n)))
    items = list(_REGIME_HISTORY)[-n:]
    dist = dict(Counter(items)) if items else {}

    return {
        "enabled": bool(st.get("is_enabled")),
        "risk_multiplier": float(st.get("risk_multiplier", 1.0)),
        "updated_at": st.get("updated_at"),
        "open_positions_count": int(open_positions_count),
        "daily_realized_pnl": float(daily_realized_pnl),
        "paper_equity_usdt": None if equity is None else float(equity),
        "daily_loss_limit_usdt": None if limit is None else float(limit),
        "regime_distribution_last_n": {"n": len(items), "dist": dist},
    }


@app.post("/control/stop")
def control_stop():
    if not db:
        raise HTTPException(status_code=500, detail="DB hazır değil")
    db.set_bot_enabled(False)
    return {"ok": True, "state": db.get_bot_state()}


@app.post("/control/start")
def control_start():
    if not db:
        raise HTTPException(status_code=500, detail="DB hazır değil")
    db.set_bot_enabled(True)
    return {"ok": True, "state": db.get_bot_state()}


@app.post("/control/risk")
def control_risk(multiplier: float):
    if not db:
        raise HTTPException(status_code=500, detail="DB hazır değil")
    db.set_risk_multiplier(multiplier)
    return {"ok": True, "state": db.get_bot_state()}


@app.post("/control/panic_close_all")
def control_panic_close_all():
    if not db or not trader_bot:
        raise HTTPException(status_code=500, detail="Servisler hazır değil")

    db.set_bot_enabled(False)
    positions = db.get_open_positions()
    if not positions:
        return {"ok": True, "message": "No open positions", "closed": [], "failed": [], "state": db.get_bot_state()}

    closed, failed = [], []
    for p in positions:
        sym = normalize_symbol(p.get("symbol"))
        amt = float(p.get("amount", 0.0))
        if not sym or amt <= 0:
            continue

        try:
            px = _safe_price(sym)
            if px is None:
                raise RuntimeError("price=0")

            trader_bot.execute_trade(
                symbol=sym,
                signal="SELL",
                current_price=px,
                risk_multiplier=1.0,
                reason="PANIC_CLOSE_ALL",
            )
            closed.append({"symbol": sym, "amount": amt})
        except Exception as e:
            failed.append({"symbol": sym, "amount": amt, "error": str(e)})

    return {"ok": True, "closed": closed, "failed": failed, "state": db.get_bot_state()}


async def run_analysis(symbol: str, timeframe: str, execute_trade: bool = False, risk_multiplier: float = 1.0):
    global _last_trade_ts

    try:
        if not binance_svc or not sentiment_svc or not strategy or not trader_bot:
            return {"symbol": symbol, "error": "Servisler hazır değil"}

        ohlcv = await asyncio.to_thread(binance_svc.get_historical_data, symbol, timeframe, limit=200)
        if not ohlcv:
            return {"symbol": symbol, "error": "Veri Yok"}

        sentiment_score = sentiment_svc.get_sentiment(symbol)

        try:
            ts_hour = int(time.time() // 3600 * 3600)
            if db and hasattr(sentiment_svc, "get_sentiment_with_meta"):
                meta = sentiment_svc.get_sentiment_with_meta(symbol)
                db.upsert_sentiment_snapshot(
                    symbol=meta["symbol"],
                    ts=ts_hour,
                    score=meta["score"],
                    analyzed=meta.get("analyzed", 0),
                    important=meta.get("important", 0),
                    pos_votes=meta.get("pos_votes", 0),
                    neg_votes=meta.get("neg_votes", 0),
                    source=meta.get("source", "cryptopanic"),
                )
        except Exception:
            pass

        profile = _resolve_profile(symbol)

        dir_1h = "UNKNOWN"
        try:
            if trend_bias_svc is not None:
                st = await asyncio.wait_for(trend_bias_svc.get(symbol), timeout=2.5)
                d = getattr(st, "direction", "UNKNOWN")
                dir_1h = str(d or "UNKNOWN").upper()
        except Exception:
            dir_1h = "UNKNOWN"

        signal_data = strategy.get_signal(
            ohlcv,
            sentiment_score,
            symbol=symbol,
            profile=profile,
            trend_dir_1h=dir_1h,
        )
        if str(signal_data.get("signal", "HOLD")).upper() == "HOLD" and snapback_strategy is not None:
            snapback_result = snapback_strategy.get_signal(
                ohlcv,
                sentiment_score,
                symbol=symbol,
                profile=profile,
                trend_dir_1h=dir_1h,
            )
            snapback_signal = str(snapback_result.get("signal", "HOLD")).upper()
            snapback_strategy_name = str(snapback_result.get("strategy_name") or "snapback")
            snapback_score = float(snapback_result.get("score", 0.0) or 0.0)
            if snapback_signal == "BUY":
                logger.info(
                    "fallback_override strategy=%s symbol=%s signal=%s score=%.2f gate_status=%s hold_fails=%s reason=%s",
                    snapback_strategy_name,
                    symbol,
                    snapback_signal,
                    snapback_score,
                    snapback_result.get("gate_status"),
                    snapback_result.get("hold_fail_reasons"),
                    str(snapback_result.get("reason", ""))[:300],
                )
                signal_data = snapback_result
            else:
                logger.info(
                    "fallback_evaluated strategy=%s symbol=%s signal=%s score=%.2f gate_status=%s hold_fails=%s reason=%s",
                    snapback_strategy_name,
                    symbol,
                    snapback_signal,
                    snapback_score,
                    snapback_result.get("gate_status"),
                    snapback_result.get("hold_fail_reasons"),
                    str(snapback_result.get("reason", ""))[:300],
                )
        signal_data = dict(signal_data)
        signal_data.setdefault("strategy_name", "breakout")
        decision = signal_data.get("signal", "HOLD")
        current_price = float(signal_data.get("current_price", 0) or 0.0)
        reason = str(signal_data.get("reason", "AUTO_SIGNAL"))

        risk_multiplier = max(0.0, min(1.0, float(risk_multiplier)))

        result = {
            "symbol": symbol,
            "price": current_price,
            "sentiment": round(float(sentiment_score), 2),
            "decision": decision,
            "risk_multiplier": risk_multiplier,
            "details": signal_data,
        }

        try:
            if db and IS_TEST_MODE:
                result["paper_usdt"] = float(db.get_paper_balance("USDT"))
                result["paper_equity_usdt"] = _paper_equity_usdt()
        except Exception:
            pass

        blocked_reason = None

        # spot-safe SELL gate
        sell_allowed = True
        if decision == "SELL":
            sell_allowed = _spot_has_open_position(symbol)
            if not sell_allowed:
                blocked_reason = "no_open_position"
                result["decision"] = "SELL(blocked)"
                try:
                    result["details"]["spot_blocked_reason"] = "no_open_position"
                except Exception:
                    pass

        # correlation-aware risk for NEW BUY
        corr_factor = 1.0
        corr_reason = None
        if execute_trade and decision == "BUY" and db and correlation_svc is not None:
            try:
                open_pos = db.get_open_positions()
                open_syms = [normalize_symbol(p.get("symbol")) for p in open_pos if p.get("symbol")]
                open_syms = [s for s in open_syms if s and s != normalize_symbol(symbol)]
                if open_syms:
                    st = await asyncio.wait_for(
                        correlation_svc.get([normalize_symbol(symbol)] + open_syms),
                        timeout=4.0,
                    )
                    cfg = getattr(correlation_svc, "_cfg", None)
                    warn = float(getattr(cfg, "corr_warn", 0.75))
                    block = float(getattr(cfg, "corr_block", 0.90))
                    corr_factor, corr_reason = correlation_penalty(
                        st=st,
                        candidate_symbol=normalize_symbol(symbol),
                        open_symbols=open_syms,
                        warn=warn,
                        block=block,
                    )
                    if corr_factor <= 0:
                        blocked_reason = corr_reason or "corr_block"
            except Exception:
                pass

        result["corr_factor"] = float(corr_factor)
        result["corr_reason"] = corr_reason

        if execute_trade and (not current_price or current_price <= 0):
            blocked_reason = "price_invalid"

        now = time.time()
        if execute_trade and COOLDOWN_SECONDS > 0 and (now - _last_trade_ts) < COOLDOWN_SECONDS:
            blocked_reason = f"cooldown({int(COOLDOWN_SECONDS - (now - _last_trade_ts))}s)"

        if execute_trade and blocked_reason is None:
            try:
                if db:
                    open_pos = db.get_open_positions()
                    if len(open_pos) >= MAX_OPEN_POSITIONS:
                        blocked_reason = f"max_open_positions({MAX_OPEN_POSITIONS})"
            except Exception:
                pass

        if execute_trade and blocked_reason is None and decision == "BUY":
            try:
                strategy_name = str(signal_data.get("strategy_name", ""))
                if db:
                    open_positions = db.get_open_positions()
                    if strategy_name == "grid_v1":
                        # Grid: check per-symbol limit
                        symbol_positions = [p for p in open_positions if p.get("symbol") == symbol]
                        max_per_symbol = int(os.getenv("GRID_MAX_POSITIONS_PER_SYMBOL", "5"))
                        if len(symbol_positions) >= max_per_symbol:
                            blocked_reason = f"grid_max_per_symbol({len(symbol_positions)}/{max_per_symbol})"
                            logger.info(
                                "grid_limit_check symbol=%s open=%d max=%d blocked=true",
                                symbol,
                                len(symbol_positions),
                                max_per_symbol,
                            )
                    else:
                        # Directional strategies: 1 position per symbol
                        if db.get_open_position(symbol):
                            blocked_reason = "position_already_open"
            except Exception:
                pass

        # Re-entry guard for directional strategies (non-grid BUY)
        if execute_trade and blocked_reason is None and decision == "BUY":
            strategy_name = str(signal_data.get("strategy_name", ""))
            if strategy_name != "grid_v1":
                can_buy_flag, block_reason = re_entry_guard.can_buy(symbol, current_price)
                if not can_buy_flag:
                    blocked_reason = f"re_entry_guard({block_reason})"
                    logger.info("⛔ RE-ENTRY GUARD BLOCKED: %s - %s", symbol, block_reason)

        can_execute = (
            execute_trade
            and decision in ["BUY", "SELL"]
            and risk_multiplier > 0
            and blocked_reason is None
            and (decision == "BUY" or (decision == "SELL" and sell_allowed))
        )

        if can_execute:
            trade_result = trader_bot.execute_trade(
                symbol=symbol,
                signal=decision,
                current_price=current_price,
                risk_multiplier=float(risk_multiplier) * float(corr_factor),
                reason="AUTO_SIGNAL",
                stop_loss=signal_data.get("stop_loss"),
                take_profit=signal_data.get("take_profit"),
                equity_usdt=(_paper_equity_usdt() if IS_TEST_MODE else None),
                signal_details=signal_data,
            )
            if trade_result:
                _last_trade_ts = time.time()
                if decision == "SELL":
                    try:
                        position = db.get_open_position(symbol) if db else None
                        if position:
                            re_entry_guard.mark_sell(
                                symbol=symbol,
                                sell_price=current_price,
                                entry_price=position.get("entry_price", 0),
                                reason=signal_data.get("reason", "unknown"),
                            )
                    except Exception:
                        pass
            else:
                blocked_reason = "trader_rejected"
                logger.info(
                    "analysis trade_rejected symbol=%s decision=%s price=%.8f risk_multiplier=%.4f reason=%s",
                    symbol,
                    decision,
                    float(current_price),
                    float(risk_multiplier) * float(corr_factor),
                    reason,
                )
            result["trade_result"] = trade_result
        else:
            if execute_trade and decision in ["BUY", "SELL"] and risk_multiplier > 0:
                result["trade_result"] = None

        if blocked_reason:
            result["blocked_reason"] = blocked_reason
            if execute_trade and decision in ["BUY", "SELL"]:
                logger.info(
                    "analysis trade_blocked symbol=%s decision=%s blocked_reason=%s price=%.8f risk_multiplier=%.4f corr_factor=%.4f",
                    symbol,
                    decision,
                    blocked_reason,
                    float(current_price),
                    float(risk_multiplier),
                    float(corr_factor),
                )

        return result

    except TradingPausedError:
        raise
    except Exception as e:
        logger.error("Analiz hatası (%s): %s", symbol, str(e)[:400])
        return {"symbol": symbol, "error": "Analiz Hatası"}


# If you run module directly
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=False, log_level="info")