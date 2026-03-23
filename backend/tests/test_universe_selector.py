import unittest


from backend.universe_selector import UniverseConfig, UniverseSelector


class _StubExchange:
    def __init__(self):
        # minimal ccxt-like
        self.markets = {
            "BTC/USDT": {"base": "BTC", "active": True},
            "ETH/USDT": {"base": "ETH", "active": True},
            "AAA/USDT": {"base": "AAA", "active": True},
            "BBB/USDT": {"base": "BBB", "active": True},
            "DOGE/USDT": {"base": "DOGE", "active": True},
            "PAXG/USDT": {"base": "PAXG", "active": True},
            "USDC/USDT": {"base": "USDC", "active": True},
            "FDUSD/USDT": {"base": "FDUSD", "active": True},
        }
        self.fetch_tickers_called_with = None

    def fetch_tickers(self, symbols=None):
        self.fetch_tickers_called_with = symbols
        # Provide just enough fields used by selector
        out = {}
        symbols = list(symbols or self.markets.keys())
        for s in symbols:
            if s == "AAA/USDT":
                out[s] = {"quoteVolume": 60_000_000, "bid": 99, "ask": 101}
            elif s == "BBB/USDT":
                out[s] = {"quoteVolume": 80_000_000, "bid": 99.5, "ask": 100.5}
            elif s == "BTC/USDT":
                out[s] = {"quoteVolume": 10_000_000_000, "bid": 50000, "ask": 50010}
            elif s == "ETH/USDT":
                out[s] = {"quoteVolume": 5_000_000_000, "bid": 2500, "ask": 2502}
            else:
                out[s] = {"quoteVolume": 10_000, "bid": 1, "ask": 2}
        return out

    def fetch_ohlcv(self, symbol, timeframe, since=None, limit=600):
        # Generate a simple trending series for BBB and a choppy series for AAA
        ohlcv = []
        price = 100.0
        for i in range(limit):
            if symbol == "BBB/USDT":
                price += 0.05
            elif symbol == "AAA/USDT":
                price += (1 if i % 2 == 0 else -1) * 0.2
            else:
                price += 0.01
            ohlcv.append([i * 3600_000, price, price * 1.01, price * 0.99, price, 1000])
        return ohlcv

    def market(self, symbol):
        return {"symbol": symbol, "id": symbol.replace("/", "")}


class UniverseSelectorTests(unittest.TestCase):
    def test_iter_usdt_symbols_applies_default_exclusions(self):
        ex = _StubExchange()
        sel = UniverseSelector(exchange=ex, cfg=UniverseConfig())
        syms = sel._iter_usdt_symbols()
        self.assertNotIn("DOGE/USDT", syms)
        self.assertNotIn("PAXG/USDT", syms)
        self.assertNotIn("USDC/USDT", syms)
        self.assertNotIn("FDUSD/USDT", syms)

    def test_rebuild_returns_anchors_and_dynamic(self):
        ex = _StubExchange()
        cfg = UniverseConfig(dynamic_n=1, min_quote_volume_usd=50_000_000.0, max_spread_pct=5.0)
        sel = UniverseSelector(exchange=ex, cfg=cfg)
        pick = sel.rebuild()
        self.assertIn("BTC/USDT", pick.symbols)
        self.assertIn("ETH/USDT", pick.symbols)
        # dynamic should include BBB (more trend-friendly than AAA in stub)
        self.assertIn("BBB/USDT", pick.symbols)
        self.assertIsNone(ex.fetch_tickers_called_with)

    def test_rebuild_matches_raw_binance_ticker_keys(self):
        class _RawTickerExchange(_StubExchange):
            def fetch_tickers(self, symbols=None):
                self.fetch_tickers_called_with = symbols
                return {
                    "BTCUSDT": {"quoteVolume": 10_000_000_000, "bid": 50000, "ask": 50010},
                    "ETHUSDT": {"quoteVolume": 5_000_000_000, "bid": 2500, "ask": 2502},
                    "AAAUSDT": {"quoteVolume": 60_000_000, "bid": 99, "ask": 101},
                    "BBBUSDT": {"quoteVolume": 80_000_000, "bid": 99.5, "ask": 100.5},
                }

        ex = _RawTickerExchange()
        cfg = UniverseConfig(dynamic_n=1, min_quote_volume_usd=50_000_000.0, max_spread_pct=5.0)
        sel = UniverseSelector(exchange=ex, cfg=cfg)
        pick = sel.rebuild()
        self.assertIn("BBB/USDT", pick.symbols)
        self.assertIsNone(ex.fetch_tickers_called_with)


if __name__ == "__main__":
    unittest.main()
