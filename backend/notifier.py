# File: backend/notifier.py
from __future__ import annotations

import asyncio
import logging
import os
import socket
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

import aiohttp
from .config import settings

logger = logging.getLogger(__name__)

_DNS_ERROR_FRAGMENTS = (
    "getaddrinfo failed",
    "name or service not known",
    "temporary failure in name resolution",
    "nodename nor servname provided",
    "no address associated with hostname",
)


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "").strip() or "").lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return bool(default)


@dataclass(frozen=True)
class TelegramConfig:
    token: str
    chat_id: str
    enabled: bool
    timeout_s: float = 10.0
    max_retries: int = 3
    backoff_base_s: float = 0.8
    backoff_cap_s: float = 20.0
    disabled_reason: str = ""
    proxy: str = ""          # HTTP proxy: "http://127.0.0.1:8080"
    proxy_base: str = ""     # Cloudflare Worker URL: "https://telegram-proxy.xxx.workers.dev"

    @staticmethod
    def from_env() -> "TelegramConfig":
        token = (os.getenv("TELEGRAM_BOT_TOKEN", "").strip() or settings.TELEGRAM_BOT_TOKEN or "")
        chat_id = (os.getenv("TELEGRAM_CHAT_ID", "").strip() or settings.TELEGRAM_CHAT_ID or "")
        enabled_by_env = _env_bool("TELEGRAM_ENABLED", default=bool(settings.TELEGRAM_ENABLED))
        enabled = enabled_by_env and bool(token and chat_id)
        disabled_reason = ""
        if not enabled_by_env:
            disabled_reason = "env_disabled"
        elif not (token and chat_id):
            disabled_reason = "missing_credentials"
        timeout_s = float(os.getenv("TELEGRAM_TIMEOUT_S", "10") or 10)
        max_retries = int(os.getenv("TELEGRAM_MAX_RETRIES", "3") or 3)
        backoff_base_s = float(os.getenv("TELEGRAM_BACKOFF_BASE_S", "0.8") or 0.8)
        backoff_cap_s = float(os.getenv("TELEGRAM_BACKOFF_CAP_S", "20") or 20)
        proxy = (os.getenv("TELEGRAM_PROXY", "").strip() or "")
        proxy_base = (os.getenv("TELEGRAM_PROXY_BASE", "").strip() or "")
        return TelegramConfig(
            token=token,
            chat_id=chat_id,
            enabled=enabled,
            timeout_s=timeout_s,
            max_retries=max_retries,
            backoff_base_s=backoff_base_s,
            backoff_cap_s=backoff_cap_s,
            disabled_reason=disabled_reason,
            proxy=proxy,
            proxy_base=proxy_base,
        )


class TelegramNotifier:
    """
    Telegram notifier (IPv4-forced) using aiohttp.
    Rationale:
      - Your environment's httpx doesn't support AsyncHTTPTransport(family=...)
      - IPv6 is broken on your network; forcing AF_INET avoids AAAA connect failures.
    """

    def __init__(self) -> None:
        self.cfg = TelegramConfig.from_env()
        self.enabled: bool = bool(self.cfg.enabled)
        self._session: Optional[aiohttp.ClientSession] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Failure / breaker
        self._consecutive_fails: int = 0
        self._breaker_until_ts: float = 0.0

        # Optional callback for truly critical failures
        self.on_critical: Optional[Callable[[str], None]] = None

    async def start(self) -> None:
        if not self.enabled:
            if self.cfg.disabled_reason == "env_disabled":
                logger.info("Telegram notifier disabled via TELEGRAM_ENABLED=false; network calls suppressed.")
            else:
                logger.info("Telegram notifier disabled: missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID.")
            return

        self._loop = asyncio.get_running_loop()
        # Force IPv4 resolver/connector
        timeout = aiohttp.ClientTimeout(total=float(self.cfg.timeout_s))
        import ssl as _ssl
        try:
            import certifi as _certifi
            _ssl_ctx = _ssl.create_default_context(cafile=_certifi.where())
        except Exception:
            # certifi yok veya hata — SSL doğrulamayı devre dışı bırak
            _ssl_ctx = False
        connector = aiohttp.TCPConnector(
            family=socket.AF_INET,  # ✅ force IPv4
            ssl=_ssl_ctx,
            limit=20,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        if self.cfg.proxy_base:
            proxy_info = f" via_worker={self.cfg.proxy_base}"
        elif self.cfg.proxy:
            proxy_info = f" proxy={self.cfg.proxy}"
        else:
            proxy_info = ""
        logger.info("Telegram notifier enabled (IPv4 forced).%s", proxy_info)

    async def stop(self) -> None:
        s = self._session
        self._session = None
        self._loop = None
        if s is not None:
            try:
                await s.close()
            except Exception:
                pass

    def _schedule(self, coro: Awaitable[bool]) -> None:
        if not self.enabled:
            try:
                coro.close()
            except Exception:
                pass
            return

        loop = self._loop
        if loop is None or loop.is_closed():
            logger.warning("Telegram notifier loop unavailable; dropping notification.")
            try:
                coro.close()
            except Exception:
                pass
            return

        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        try:
            if running_loop is loop:
                loop.create_task(coro)
            else:
                asyncio.run_coroutine_threadsafe(coro, loop)
        except Exception:
            logger.exception("Telegram notification scheduling failed")
            try:
                coro.close()
            except Exception:
                pass

    def _in_breaker(self) -> bool:
        import time

        return time.time() < float(self._breaker_until_ts or 0.0)

    def _set_breaker(self, seconds: float) -> None:
        import time

        self._breaker_until_ts = time.time() + float(seconds)

    def _backoff_delay(self, attempt: int) -> float:
        # attempt starts from 1
        base = float(self.cfg.backoff_base_s)
        cap = float(self.cfg.backoff_cap_s)
        delay = base * (2 ** max(0, int(attempt) - 1))
        return float(min(cap, delay))

    def _is_dns_error(self, error: BaseException) -> bool:
        if isinstance(error, socket.gaierror):
            return True

        os_error = getattr(error, "os_error", None)
        if isinstance(os_error, socket.gaierror):
            return True

        current: BaseException | None = error
        while current is not None:
            if isinstance(current, socket.gaierror):
                return True
            message = str(current).lower()
            if any(fragment in message for fragment in _DNS_ERROR_FRAGMENTS):
                return True
            current = getattr(current, "__cause__", None)
        return False

    async def send_message(self, text: str) -> bool:
        if not self.enabled:
            return False
        if self._session is None:
            # In case start() wasn't called yet (shouldn't happen in your wiring, but safe)
            await self.start()
            if self._session is None:
                return False

        if self._in_breaker():
            return False

        # URL: Worker proxy varsa Worker URL'ini kullan, yoksa direkt Telegram
        if self.cfg.proxy_base:
            # Cloudflare Worker üzerinden — DNS sorununu bypass etmek için
            # önce normal URL, hata alırsa IP ile dene
            _base = self.cfg.proxy_base.rstrip("/")
            url = f"{_base}/bot{self.cfg.token}/sendMessage"
            # IP bazlı fallback URL (DNS çözümlemesi başarısız olursa)
            _base_ip = _base.replace(
                "telegram-proxy.tetik03yusuf.workers.dev",
                "104.21.12.224"
            )
            _url_ip = f"{_base_ip}/bot{self.cfg.token}/sendMessage"
            _host_header = "telegram-proxy.tetik03yusuf.workers.dev"
            _http_proxy = None
        else:
            url = f"https://api.telegram.org/bot{self.cfg.token}/sendMessage"
            _http_proxy = self.cfg.proxy or None

        payload = {"chat_id": self.cfg.chat_id, "text": text}

        last_err: Optional[str] = None
        dns_issue_logged = False

        for attempt in range(1, int(self.cfg.max_retries) + 1):
            try:
                # requests kütüphanesi ile gönder (Windows SSL sorununu bypass eder)
                import requests as _requests
                import functools
                import urllib3; urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                _proxies = {"http": _http_proxy, "https": _http_proxy} if _http_proxy else None
                _resp_sync = await asyncio.get_event_loop().run_in_executor(
                    None,
                    functools.partial(
                        _requests.post,
                        url,
                        json=payload,
                        timeout=float(self.cfg.timeout_s),
                        proxies=_proxies,
                        verify=False,
                    )
                )
                # Fake async context manager yerine direkt işle
                body = _resp_sync.text
                if 200 <= _resp_sync.status_code < 300:
                    self._consecutive_fails = 0
                    return True
                if _resp_sync.status_code in (400, 401, 403):
                    last_err = f"HTTP {_resp_sync.status_code}: {body[:300]}"
                    logger.error("❌ Telegram permanent error: %s", last_err)
                    self.enabled = False
                    return False
                last_err = f"HTTP {_resp_sync.status_code}: {body[:200]}"
                logger.warning("telegram_http_error attempt=%d status=%d", attempt, _resp_sync.status_code)
                continue
            except Exception as _req_err:
                last_err = str(_req_err)[:200]
                logger.warning("Telegram send failed (attempt=%d/%d, fails=%d): %s",
                    attempt, int(self.cfg.max_retries),
                    self._consecutive_fails + 1, last_err)
                self._consecutive_fails += 1
                if attempt < int(self.cfg.max_retries):
                    _sleep = min(self.cfg.backoff_base_s * (2 ** (attempt - 1)), self.cfg.backoff_cap_s)
                    await asyncio.sleep(_sleep)
                continue
            except Exception as e:
                last_err = str(e)[:300]
                self._consecutive_fails += 1
                dns_error = self._is_dns_error(e)
                if dns_error:
                    if not dns_issue_logged:
                        dns_issue_logged = True
                        logger.warning(
                            "Telegram DNS resolution failed; retrying with breaker protection "
                            "(attempt=%s/%s, fails=%s): %s",
                            attempt,
                            self.cfg.max_retries,
                            self._consecutive_fails,
                            last_err,
                        )
                elif attempt == 1 or attempt >= int(self.cfg.max_retries):
                    logger.warning(
                        "Telegram send failed (attempt=%s/%s, fails=%s): %s",
                        attempt,
                        self.cfg.max_retries,
                        self._consecutive_fails,
                        last_err,
                    )

                # breaker after final attempt
                if attempt >= int(self.cfg.max_retries):
                    delay = self._backoff_delay(self._consecutive_fails)
                    self._set_breaker(delay)
                    if dns_error:
                        logger.warning(
                            "Telegram DNS unavailable; breaker active for %.1fs (fails=%s).",
                            delay,
                            self._consecutive_fails,
                        )
                    else:
                        logger.error(
                            "Telegram message could not be sent (fails=%s, breaker=%.1fs). Last=%s",
                            self._consecutive_fails,
                            delay,
                            last_err,
                        )
                    return False

                await asyncio.sleep(self._backoff_delay(attempt))

        return False

    async def send_error(self, title: str, detail: str) -> bool:
        msg = f"⚠️ *{title}*\n`{detail}`"
        return await self.send_message(msg)

    async def send_info(self, title: str, detail: str = "") -> bool:
        msg = f"ℹ️ *{title}*"
        if detail:
            msg += f"\n{detail}"
        return await self.send_message(msg)

    async def send_startup_alert(
        self,
        *,
        mode: str,
        symbols: list[str],
        grid_lower: float | None = None,
        grid_upper: float | None = None,
        paper_balance: float | None = None,
    ) -> bool:
        """Bot başlatıldığında gönderilir."""
        try:
            mode_emoji = "🧪" if mode == "test" else "🚀"
            lines = [
                f"{mode_emoji} *Bot Başlatıldı* [{mode.upper()}]",
                f"📊 Semboller: {', '.join(symbols)}",
            ]
            if grid_lower and grid_upper:
                lines.append(f"📐 Grid aralığı: ${grid_lower:,.0f} — ${grid_upper:,.0f}")
            if paper_balance is not None:
                lines.append(f"💰 Bakiye: ${paper_balance:,.2f} USDT")
            return await self.send_message("\n".join(lines))
        except Exception:
            logger.exception("send_startup_alert failed")
            return False

    async def send_daily_summary(
        self,
        *,
        realized_pnl: float,
        open_positions: int,
        total_trades: int,
        equity: float,
        win_rate: float | None = None,
    ) -> bool:
        """Günlük özet (istenen saatte çağrılabilir)."""
        try:
            pnl_emoji = "🟢" if realized_pnl >= 0 else "🔴"
            lines = [
                f"📋 *Günlük Özet*",
                f"{pnl_emoji} Gerçekleşen PnL: ${realized_pnl:+.2f}",
                f"📈 Toplam işlem: {total_trades}",
                f"📂 Açık pozisyon: {open_positions}",
                f"💼 Equity: ${equity:,.2f}",
            ]
            if win_rate is not None:
                lines.append(f"🎯 Win rate: {win_rate*100:.1f}%")
            return await self.send_message("\n".join(lines))
        except Exception:
            logger.exception("send_daily_summary failed")
            return False

    async def send_grid_event(
        self,
        *,
        symbol: str,
        event: str,
        price: float,
        pnl: float | None = None,
        regime: str | None = None,
        grid_rounds: int | None = None,
    ) -> bool:
        """Grid özel olayları: emergency exit, trend close, round tamamlandı."""
        try:
            event_map = {
                "GRID_EMERGENCY_EXIT": "🚨 Emergency Exit",
                "GRID_TREND_CLOSE":    "🛑 Trend Kapat",
                "GRID_ROUND_COMPLETE": "✅ Round Tamamlandı",
                "GRID_REBALANCE":      "🔄 Grid Yeniden Kuruldu",
            }
            label = event_map.get(event, f"⚡ {event}")
            lines = [f"{label}  *{symbol}*", f"💲 Fiyat: ${price:,.2f}"]
            if pnl is not None:
                pnl_emoji = "🟢" if pnl >= 0 else "🔴"
                lines.append(f"{pnl_emoji} PnL: ${pnl:+.4f}")
            if regime:
                lines.append(f"📡 Rejim: {regime}")
            if grid_rounds is not None:
                lines.append(f"🔁 Toplam round: {grid_rounds}")
            return await self.send_message("\n".join(lines))
        except Exception:
            logger.exception("send_grid_event failed")
            return False

    async def send_trade_alert(
        self,
        *,
        symbol: str,
        side: str,
        amount: float,
        price: float,
        pnl: float | None = None,
        pnl_pct: float | None = None,
        is_test: bool = False,
        strategy_name: str | None = None,
        regime: str | None = None,
        reason: str | None = None,
    ) -> bool:
        try:
            mode_tag = " [TEST]" if is_test else ""
            side_u = str(side).upper()

            if side_u == "BUY":
                side_emoji = "🟢 BUY"
            elif side_u == "SELL":
                if pnl is not None and pnl >= 0:
                    side_emoji = "💰 SELL ✅"
                elif pnl is not None:
                    side_emoji = "🔴 SELL ❌"
                else:
                    side_emoji = "🔵 SELL"
            else:
                side_emoji = f"⚡ {side_u}"

            lines = [
                f"{side_emoji}  *{symbol}*{mode_tag}",
                f"💲 ${float(price):,.4f}   📦 {float(amount):.6f}",
            ]

            if pnl is not None:
                pnl_str = f"${pnl:+.4f}"
                if pnl_pct is not None:
                    pnl_str += f"  ({pnl_pct:+.2f}%)"
                lines.append(f"{'🟢' if pnl >= 0 else '🔴'} PnL: {pnl_str}")

            meta = []
            if strategy_name:
                meta.append(strategy_name)
            if regime and regime != "UNKNOWN":
                meta.append(f"rejim={regime}")
            if reason:
                meta.append(reason[:40])
            if meta:
                lines.append(f"📎 {' · '.join(meta)}")

            return await self.send_message("\n".join(lines))
        except Exception:
            logger.exception("send_trade_alert failed")
            return False

    async def send_paused_alert(self, reason: str, detail: str = "") -> bool:
        msg = f"PAUSED: {reason}"
        if detail:
            msg = f"{msg}\n{detail}"
        return await self.send_message(msg)

    def notify_trade_alert(
        self,
        *,
        symbol: str,
        side: str,
        amount: float,
        price: float,
        pnl: float | None = None,
        pnl_pct: float | None = None,
        is_test: bool = False,
        strategy_name: str | None = None,
        regime: str | None = None,
        reason: str | None = None,
    ) -> None:
        if not self.enabled:
            return
        self._schedule(
            self.send_trade_alert(
                symbol=symbol,
                side=side,
                amount=amount,
                price=price,
                pnl=pnl,
                pnl_pct=pnl_pct,
                is_test=is_test,
                strategy_name=strategy_name,
                regime=regime,
                reason=reason,
            )
        )

    def notify_grid_event(
        self,
        *,
        symbol: str,
        event: str,
        price: float,
        pnl: float | None = None,
        regime: str | None = None,
        grid_rounds: int | None = None,
    ) -> None:
        if not self.enabled:
            return
        self._schedule(
            self.send_grid_event(
                symbol=symbol,
                event=event,
                price=price,
                pnl=pnl,
                regime=regime,
                grid_rounds=grid_rounds,
            )
        )

    def notify_paused_alert(self, reason: str, detail: str = "") -> None:
        if not self.enabled:
            return
        self._schedule(self.send_paused_alert(reason=reason, detail=detail))