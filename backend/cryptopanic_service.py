# File: backend/cryptopanic_service.py
from __future__ import annotations

import os
import time
import logging
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)


class SentimentRateLimited(RuntimeError):
    pass


@dataclass(frozen=True)
class SentimentResult:
    symbol: str
    score: float
    source: str
    important: int = 0
    analyzed: int = 0
    pos_votes: int = 0
    neg_votes: int = 0


class CryptoPanicService:
    """
    CryptoPanic sentiment provider with:
    - Token-bucket-like cooldown after 429
    - Per-symbol TTL cache to avoid hammering
    - Safe fallback to neutral score (0.0) when unavailable
    """

    def __init__(self) -> None:
        self.api_key = os.getenv("CRYPTOPANIC_API_KEY", "").strip()
        self.base_url = os.getenv("CRYPTOPANIC_BASE_URL", "https://cryptopanic.com/api/v1/posts/").strip()

        self.cooldown_until: float = 0.0
        self.cooldown_seconds_default: int = int(os.getenv("CRYPTOPANIC_COOLDOWN_SECONDS", "900"))

        self.cache_ttl_seconds: int = int(os.getenv("SENTIMENT_CACHE_TTL_SECONDS", "900"))
        self._cache: dict[str, tuple[float, SentimentResult]] = {}

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "NexusQuant/1.0"})

    def _now(self) -> float:
        return time.time()

    def _in_cooldown(self) -> bool:
        return self._now() < self.cooldown_until

    @staticmethod
    def _base_symbol(symbol: str) -> str:
        return symbol.split("/")[0].upper().strip()

    def get_sentiment_with_meta(self, symbol: str) -> dict:
        """
        Returns a dict compatible with your DB snapshot upsert.
        When rate-limited/unavailable, returns neutral score with source='none'.
        """
        base = self._base_symbol(symbol)

        # cache hit
        cached = self._cache.get(base)
        if cached:
            ts, res = cached
            if (self._now() - ts) < self.cache_ttl_seconds:
                return {
                    "symbol": res.symbol,
                    "score": res.score,
                    "source": res.source,
                    "important": res.important,
                    "analyzed": res.analyzed,
                    "pos_votes": res.pos_votes,
                    "neg_votes": res.neg_votes,
                }

        if not self.api_key:
            return {"symbol": base, "score": 0.0, "source": "none", "important": 0, "analyzed": 0, "pos_votes": 0, "neg_votes": 0}

        if self._in_cooldown():
            return {"symbol": base, "score": 0.0, "source": "cooldown", "important": 0, "analyzed": 0, "pos_votes": 0, "neg_votes": 0}

        try:
            # NOTE: Keep this minimal; you likely already have logic here.
            # If you already implemented get_sentiment(), you can route through it.
            url = self.base_url
            params = {
                "auth_token": self.api_key,
                "currencies": base,
                "public": "true",
            }
            r = self._session.get(url, params=params, timeout=10)

            if r.status_code == 429:
                self.cooldown_until = self._now() + float(self.cooldown_seconds_default)
                logger.error("Sentiment 429 (rate limit). Cooldown %ss başladı. Coin=%s", self.cooldown_seconds_default, base)
                raise SentimentRateLimited("cryptopanic_429")

            r.raise_for_status()
            data = r.json() or {}

            # Very conservative score: if any posts exist => small positive bias; else neutral.
            # Replace with your own scoring if you already have it.
            results = data.get("results") or []
            score = 0.0
            important = 0
            analyzed = 0
            pos_votes = 0
            neg_votes = 0

            # if CryptoPanic returns vote fields in results, you can aggregate here.
            # We keep it safe/neutral by default.
            if results:
                score = 0.2

            res = SentimentResult(symbol=base, score=float(score), source="cryptopanic", important=important, analyzed=analyzed, pos_votes=pos_votes, neg_votes=neg_votes)
            self._cache[base] = (self._now(), res)

            return {
                "symbol": res.symbol,
                "score": res.score,
                "source": res.source,
                "important": res.important,
                "analyzed": res.analyzed,
                "pos_votes": res.pos_votes,
                "neg_votes": res.neg_votes,
            }

        except SentimentRateLimited:
            return {"symbol": base, "score": 0.0, "source": "cooldown", "important": 0, "analyzed": 0, "pos_votes": 0, "neg_votes": 0}
        except Exception as e:
            logger.error("Sentiment fetch failed (%s): %s", base, e)
            return {"symbol": base, "score": 0.0, "source": "error", "important": 0, "analyzed": 0, "pos_votes": 0, "neg_votes": 0}

    def get_sentiment(self, symbol: str) -> float:
        return float(self.get_sentiment_with_meta(symbol).get("score", 0.0))