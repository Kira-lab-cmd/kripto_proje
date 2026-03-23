# File: backend/binance_service.py
"""
CCXT wrapper with operational hardening:
  - symbol normalization
  - bounded retry/backoff on transient errors
  - endpoint rotation (Binance public URLs) on failures
  - last-known-price fallback (short TTL) to prevent total blindness during DNS/ISP issues

Callers that run inside asyncio should wrap these sync calls with
`asyncio.to_thread(...)` to avoid event-loop stalls.
"""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Any

import ccxt

from .utils_symbols import normalize_symbol

logger = logging.getLogger(__name__)


class BinanceService:
    """
    Binance bağlantısını yönetir.
    - TEST_MODE=True: sadece PUBLIC veri (ticker/ohlcv) kullanır, emir/balance kullanılmaz.
    - TEST_MODE=False: LIVE trade için API key gerekir.
    """

    def __init__(self):
        self.is_testnet = os.getenv("TEST_MODE", "True").lower() == "true"

        self.api_key = os.getenv("BINANCE_API_KEY") or ""
        self.secret_key = os.getenv("BINANCE_SECRET_KEY") or ""

        config: dict[str, Any] = {
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",
                "adjustForTimeDifference": True,
                "recvWindow": 60000,
                # Sadece spot market yükle, futures/dapi endpoint'leri atlansın
                "fetchMarkets": ["spot"],
            },
        }

        # LIVE modda key zorunlu
        if not self.is_testnet:
            if not self.api_key or not self.secret_key:
                logger.critical("❌ LIVE modda BINANCE_API_KEY veya BINANCE_SECRET_KEY eksik!")
                raise ValueError("API Anahtarları Eksik (LIVE)")
            config["apiKey"] = self.api_key
            config["secret"] = self.secret_key
            logger.info("💸 Binance LIVE modunda başlatılıyor (Gerçek Para)")
        else:
            logger.info("🧪 TEST_MODE: Binance PUBLIC veri (live endpoint) kullanılacak. Emirler simüle.")

        self.exchange = ccxt.binance(config)

        # --- NEW: endpoint pool + rotation state ---
        self._base_urls = [
            u.strip()
            for u in os.getenv(
                "BINANCE_BASE_URLS",
                "https://api.binance.com,https://api1.binance.com,https://api2.binance.com,https://api3.binance.com,https://data-api.binance.vision",
            ).split(",")
            if u.strip()
        ]
        self._base_idx = 0

        # --- NEW: last-known price cache (symbol -> (price, ts)) ---
        self._last_price: dict[str, tuple[float, float]] = {}
        self._last_price_ttl_s = int(os.getenv("LAST_PRICE_TTL_SECONDS", "120") or "120")

        # Public market yüklemesi
        try:
            self.exchange.load_markets()
            logger.info("✅ Binance piyasa verileri yüklendi.")
        except Exception as e:
            logger.critical("❌ Binance Bağlantı Hatası: %s", e)
            raise

    @staticmethod
    def _sleep_backoff(attempt: int) -> None:
        base = 0.35 * (2**attempt)
        time.sleep(min(3.0, base + random.random() * 0.15))

    def _rotate_base_url(self) -> None:
        if not self._base_urls:
            return
        self._base_idx = (self._base_idx + 1) % len(self._base_urls)
        base = self._base_urls[self._base_idx]

        # ccxt binance: urls yapısını düzgün override et
        try:
            # ccxt binance exchange'in urls['api'] dict içindeki tüm string
            # değerleri yeni base URL ile değiştir
            def _replace_urls(obj, new_base):
                if isinstance(obj, str) and obj.startswith("http"):
                    return new_base
                if isinstance(obj, dict):
                    return {k: _replace_urls(v, new_base) for k, v in obj.items()}
                return obj

            if "api" in self.exchange.urls:
                self.exchange.urls["api"] = _replace_urls(
                    self.exchange.urls["api"], base
                )
            # ccxt v4+ bazen 'urls' içinde 'api' doğrudan string olabilir
            if isinstance(self.exchange.urls.get("api"), str):
                self.exchange.urls["api"] = base
        except Exception as ex:
            logger.debug("url_rotate_err: %s", ex)

        logger.warning("binance_base_url_rotated -> %s", base)

    def _call_with_retry(self, fn, *args, **kwargs):
        max_attempts = int(os.getenv("CCXT_MAX_RETRIES", "3"))
        last_err: Exception | None = None

        for attempt in range(max_attempts):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                last_err = e
                if attempt >= max_attempts - 1:
                    break
                logger.warning(
                    "ccxt call failed (attempt=%s/%s): %s",
                    attempt + 1,
                    max_attempts,
                    str(e)[:220],
                )
                # rotate endpoint before backoff on every failure
                self._rotate_base_url()
                self._sleep_backoff(attempt)

        # final attempt exhausted
        raise last_err or RuntimeError("retry_exhausted")

    def _fetch_price_direct(self, symbol: str) -> float | None:
        """ccxt başarısız olunca doğrudan Binance REST API v3 çağrısı yapar."""
        import urllib.request
        import json as _json

        # BTC/USDT → BTCUSDT
        raw = symbol.replace("/", "").upper()
        endpoints = [
            f"https://api.binance.com/api/v3/ticker/price?symbol={raw}",
            f"https://api1.binance.com/api/v3/ticker/price?symbol={raw}",
            f"https://api2.binance.com/api/v3/ticker/price?symbol={raw}",
            f"https://data-api.binance.vision/api/v3/ticker/price?symbol={raw}",
        ]
        for url in endpoints:
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=5) as resp:
                    data = _json.loads(resp.read())
                    p = float(data.get("price", 0))
                    if p > 0:
                        return p
            except Exception:
                continue
        return None

    def get_price(self, symbol: str = "BTC/USDT") -> float | None:
        symbol_n = normalize_symbol(symbol)

        # Önce direkt REST API dene (ccxt /ticker/24hr endpoint'i 404 veriyor)
        try:
            direct_price = self._fetch_price_direct(symbol_n)
            if direct_price and direct_price > 0:
                self._last_price[symbol_n] = (direct_price, time.time())
                return direct_price
        except Exception:
            pass

        # Direkt REST başarısız → ccxt dene
        try:
            ticker = self._call_with_retry(self.exchange.fetch_ticker, symbol_n)
            p = ticker.get("last") or ticker.get("close")
            if p is None:
                raise RuntimeError(f"ticker_no_price: {ticker}")
            price = float(p)
            if price > 0:
                self._last_price[symbol_n] = (price, time.time())
            return price
        except Exception as e:
            # Son çare: önbellekteki fiyat (TTL kontrollü)
            cached = self._last_price.get(symbol_n)
            if cached:
                price, ts = cached
                if (time.time() - ts) <= float(self._last_price_ttl_s):
                    logger.warning(
                        "price_cache_fallback sym=%s price=%.4f age=%.1fs",
                        symbol_n, price, (time.time() - ts),
                    )
                    return float(price)

            logger.error("Fiyat çekme hatası (%s): %s", symbol, str(e)[:220])
            return None

    def get_ticker_price(self, symbol: str) -> float:
        price = self.get_price(symbol)
        return float(price) if price is not None else 0.0

    def _fetch_ohlcv_direct(self, symbol: str, timeframe: str = "15m", limit: int = 200) -> list | None:
        """ccxt başarısız olunca doğrudan Binance klines API çağrısı yapar."""
        import urllib.request
        import json as _json

        raw = symbol.replace("/", "").upper()
        endpoints = [
            "https://api.binance.com",
            "https://api1.binance.com",
            "https://data-api.binance.vision",
        ]
        for base in endpoints:
            try:
                url = f"{base}/api/v3/klines?symbol={raw}&interval={timeframe}&limit={limit}"
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = _json.loads(resp.read())
                    if data and isinstance(data, list) and len(data) > 10:
                        # Binance klines: [ts, open, high, low, close, volume, ...]
                        return [[int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])] for c in data]
            except Exception:
                continue
        return None

    def get_historical_data(self, symbol: str = "BTC/USDT", timeframe: str = "15m", limit: int = 200) -> list | None:
        symbol_n = normalize_symbol(symbol)

        # Önce direkt REST API dene
        try:
            ohlcv = self._fetch_ohlcv_direct(symbol_n, timeframe, limit)
            if ohlcv:
                return ohlcv
        except Exception:
            pass

        # Direkt REST başarısız → ccxt dene
        try:
            ohlcv = self._call_with_retry(self.exchange.fetch_ohlcv, symbol_n, timeframe, None, limit)
            if not ohlcv:
                logger.warning("Veri boş geldi: %s", symbol_n)
                return None
            return ohlcv
        except Exception as e:
            logger.error("Tarihsel veri hatası (%s): %s", symbol_n, str(e)[:240])
            return None