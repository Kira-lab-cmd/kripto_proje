# File: backend/utils_symbols.py
from __future__ import annotations


def normalize_symbol(symbol: str) -> str:
    """
    Canonical symbol format for ccxt/binance: 'BASE/QUOTE'
    Accepts:
      - 'BTC_USDT' -> 'BTC/USDT'
      - 'btc/usdt' -> 'BTC/USDT'
      - 'BTCUSDT'  -> 'BTC/USDT' (best-effort, only for common quote assets)
    """
    s = (symbol or "").strip()
    if not s:
        return s

    s = s.replace("-", "/").replace("_", "/").upper()

    if "/" in s:
        base, quote = s.split("/", 1)
        return f"{base.strip()}/{quote.strip()}"

    # Best-effort for concatenated symbols
    common_quotes = ("USDT", "USDC", "BUSD", "BTC", "ETH", "TRY", "EUR")
    for q in common_quotes:
        if s.endswith(q) and len(s) > len(q):
            base = s[: -len(q)]
            return f"{base}/{q}"

    return s