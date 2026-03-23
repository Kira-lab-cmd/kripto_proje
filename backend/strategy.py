# File: backend/strategy.py (CURRENT WORKING VERSION - GRID TRADING)
"""
GRID TRADING STRATEGY

Completely different paradigm from directional trading:
- NO prediction needed
- Profits from volatility
- Works in range-bound markets
- High win rate (80-90%)

Strategy:
1. Place buy/sell grids in price range
2. Buy when price crosses down through grid
3. Sell when price crosses up through grid
4. Repeat infinitely

Expected Performance:
- Win Rate: 80-90%
- Monthly Return: 5-15%
- Works best in: Sideways/ranging markets
"""

from __future__ import annotations
from pathlib import Path
import logging
import os
from typing import Any, Dict, List, Tuple, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

import pandas as pd

from backend.indicators import Indicators
from backend.regime import RegimeDetector, RegimeConfig


@dataclass
class GridLevel:
    """Single grid level"""
    price: float
    quantity: float = 0.0  # Current inventory at this grid
    buy_count: int = 0   # Times bought at this grid
    sell_count: int = 0  # Times sold at this grid
    

@dataclass
class SymbolGridState:
    """Grid state for a specific symbol"""
    symbol: str
    grids: List[GridLevel]
    last_price: Optional[float]
    execution_history: List['GridExecution']
    grid_lower: float
    grid_upper: float
    needs_rebalance: bool = False
    
    
@dataclass
class GridExecution:
    """Record of grid execution"""
    grid_price: float
    action: str  # "BUY" or "SELL"
    quantity: float
    timestamp_ms: int
    

GATE_STATUS_KEYS = (
    "price_in_range",
    "atr_ok",
    "volume_ok",
)


class TradingStrategy:
    """
    GRID TRADING STRATEGY
    
    Core Logic:
    1. Define price range (lower - upper)
    2. Split range into N grids
    3. Buy at grid levels when price drops
    4. Sell at grid levels when price rises
    5. Profit from each round trip
    
    Key Parameters:
    - GRID_LOWER_PRICE: Bottom of range (e.g., 90000)
    - GRID_UPPER_PRICE: Top of range (e.g., 100000)
    - GRID_COUNT: Number of grids (e.g., 20)
    - GRID_CAPITAL_PER_LEVEL: Capital per grid (e.g., 50 USDT)
    """

    def __init__(self, symbol: str = "BTC/USDT", initial_price: Optional[float] = None) -> None:
        """
        Initialize grid strategy with symbol-specific configuration
        
        Args:
            symbol: Trading pair (e.g., "BTC/USDT", "ETH/USDT")
            initial_price: Initial price for dynamic grid calculation (optional)
        """
        # Multi-symbol support: cache grid state per symbol
        self._symbol_grids: Dict[str, SymbolGridState] = {}
        
        # Regime detection
        self._regime_detector = RegimeDetector(RegimeConfig(
            adx_period=14,
            er_period=20,          # 20 bar daha responsive
            trend_adx_min=22.0,
            trend_er_min=0.40,
            chop_adx_max=20.0,
            chop_er_max=0.28,
            high_vol_atr_pct=0.055,
        ))

        # Per-symbol son regime cache (watchdog tarafından okunur)
        # { symbol: {"regime": str, "adx": float|None, "er": float|None,
        #             "atr_pct": float|None, "updated_at": float (epoch)} }
        self._regime_cache: Dict[str, Dict] = {}

        # Default symbol (for backward compatibility)
        self.default_symbol = symbol
        
        # Configuration (shared across all symbols)
        self.grid_count = int(os.getenv("GRID_COUNT", "100"))
        self.capital_per_grid = float(os.getenv("GRID_CAPITAL_PER_LEVEL", "50.0"))
        self.min_atr_pct = float(os.getenv("MIN_ATR_PCT", "0.003"))
        self.max_atr_pct = float(os.getenv("MAX_ATR_PCT", "0.035"))
        self.min_volume_ratio = float(os.getenv("MIN_VOLUME_RATIO", "0.5"))
        self.emergency_stop_pct = float(os.getenv("GRID_EMERGENCY_STOP_PCT", "0.15"))
        
        # Legacy single-symbol attributes (for backward compat)
        self.symbol = symbol
        self.base_asset = symbol.split("/")[0] if "/" in symbol else symbol
        self.grids: List[GridLevel] = []
        self.last_price: Optional[float] = None
        self.execution_history: List[GridExecution] = []
        self.needs_rebalance = False
        self.grid_lower = 0.0
        self.grid_upper = 0.0
        
        # Initialize default symbol
        self._init_symbol_grid(symbol, initial_price)
    
    def _init_symbol_grid(self, symbol: str, initial_price: Optional[float] = None) -> None:
        """Initialize grid for a specific symbol.

        Her sembol için ayrı GRID_WIDTH_PCT desteklenir:
          BTC_GRID_WIDTH_PCT, ETH_GRID_WIDTH_PCT vb.
        Yoksa genel GRID_WIDTH_PCT kullanılır.
        """
        base_asset = symbol.split("/")[0] if "/" in symbol else symbol

        # Sembol bazlı genişlik: ETH daha volatil → daha dar aralık tercih edilir
        # Öncelik: {BASE}_GRID_WIDTH_PCT > GRID_WIDTH_PCT > default 0.08
        sym_width_key = f"{base_asset}_GRID_WIDTH_PCT"
        grid_width_pct = float(os.getenv(sym_width_key) or os.getenv("GRID_WIDTH_PCT") or "0.08")

        # Calculate grid range for this symbol
        if initial_price is not None:
            # DYNAMIC MODE: fiyat etrafında simetrik aralık
            grid_lower = initial_price * (1 - grid_width_pct)
            grid_upper = initial_price * (1 + grid_width_pct)
        else:
            # STATIC MODE: sembol-spesifik ENV önce, sonra genel fallback
            symbol_lower_key = f"{base_asset}_GRID_LOWER"
            symbol_upper_key = f"{base_asset}_GRID_UPPER"

            grid_lower = float(os.getenv(
                symbol_lower_key,
                os.getenv("GRID_LOWER_PRICE", "60000")
            ))
            grid_upper = float(os.getenv(
                symbol_upper_key,
                os.getenv("GRID_UPPER_PRICE", "75000")
            ))
        
        # ── DİNAMİK ATR BAZLI GRID ADIMI ─────────────────────────────────
        # GRID_ATR_DYNAMIC=true ise seviyeleri eşit değil ATR'ye göre dağıt
        # Mantık: yüksek volatilitede adım büyür (daha az seviye ama daha güvenli),
        #          düşük volatilitede adım küçülür (daha sık trade fırsatı).
        # ATR bilgisi yoksa standart eşit dağılım kullanılır.
        atr_dynamic = os.getenv("GRID_ATR_DYNAMIC", "false").lower() == "true"
        step = (grid_upper - grid_lower) / self.grid_count
        grids = []

        if atr_dynamic and initial_price and initial_price > 0:
            # ATR tahminini fiyat bazlı yap (gerçek OHLCV yoksa yaklaşım)
            # Gerçek ATR: OHLCV verisi gerektirir, init sırasında henüz yok.
            # Bu yüzden coin bazlı tarihsel volatilite çarpanı kullanıyoruz.
            base = symbol.split("/")[0] if "/" in symbol else symbol
            _vol_map = {"BTC": 0.012, "ETH": 0.015, "SOL": 0.020, "BNB": 0.016}
            _atr_pct = _vol_map.get(base, 0.015)
            _atr_abs = initial_price * _atr_pct

            # Merkeze yakın seviyeleri sıkıştır (daha sık trade),
            # uç seviyeleri genişlet (güvenlik tamponu)
            mid = (grid_upper + grid_lower) / 2
            half = self.grid_count // 2
            prices = []
            for i in range(self.grid_count + 1):
                # Merkez etrafında ATR ağırlıklı dağılım
                t = (i - half) / half if half > 0 else 0
                # Merkeze yakın: küçük adım, kenarlara yakın: büyük adım
                offset = t * (grid_upper - mid) * (1 + abs(t) * _atr_pct * 5)
                prices.append(mid + offset)
            # Sınırlar içinde kal
            prices = sorted(set(max(grid_lower, min(grid_upper, p)) for p in prices))
            grids = [GridLevel(price=p) for p in prices]
            import logging as _log
            _log.getLogger(__name__).debug(
                "grid_atr_dynamic sym=%s atr_pct=%.3f levels=%d", symbol, _atr_pct, len(grids)
            )
        else:
            # Standart eşit aralık
            for i in range(self.grid_count + 1):
                price = grid_lower + (i * step)
                grids.append(GridLevel(price=price))
        # ── DİNAMİK ATR BAZLI GRID ADIMI SONU ───────────────────────────
        
        # Store in cache
        self._symbol_grids[symbol] = SymbolGridState(
            symbol=symbol,
            grids=grids,
            last_price=None,
            execution_history=[],
            grid_lower=grid_lower,
            grid_upper=grid_upper,
            needs_rebalance=False
        )
        
        # Update legacy attributes if this is default symbol
        if symbol == self.default_symbol:
            self.grids = grids
            self.grid_lower = grid_lower
            self.grid_upper = grid_upper
            
    def _get_symbol_grid(self, symbol: str) -> SymbolGridState:
        """Get or create grid state for a symbol"""
        if symbol not in self._symbol_grids:
            self._init_symbol_grid(symbol)
        return self._symbol_grids[symbol]
        
    def _initialize_grids(self) -> None:
        """Legacy wrapper - reinitialize default symbol's grids"""
        # Reinitialize default symbol
        self._init_symbol_grid(self.default_symbol)

            
    def get_config_snapshot(self) -> Dict[str, Any]:
        return {
            "version": "grid_v1",
            "grid_lower": float(self.grid_lower),
            "grid_upper": float(self.grid_upper),
            "grid_count": int(self.grid_count),
            "capital_per_grid": float(self.capital_per_grid),
            "emergency_stop_pct": float(self.emergency_stop_pct),
            "min_atr_pct": float(self.min_atr_pct),
            "max_atr_pct": float(self.max_atr_pct),
            "min_volume_ratio": float(self.min_volume_ratio),
        }

    def get_signal(
        self,
        ohlcv_data: list,
        sentiment_score: float,
        *,
        symbol: str | None = None,
        profile: dict[str, Any] | None = None,
        trend_dir_1h: str | None = None,
    ) -> Dict[str, Any]:
        """
        Backtest-compatible wrapper for analyze()
        
        This allows grid_strategy to work with existing backtest infrastructure
        that expects get_signal() instead of analyze()
        """
        # Call analyze() with the OHLCV data
        signal, sl, tp, metadata = self.analyze(
            symbol=symbol or "UNKNOWN",
            ohlcv=ohlcv_data,
            timeframe="15m"
        )
        
        current_price = metadata.get("current_price", 0)
        
        # Convert to backtest format (must have "signal" field, not "action"!)
        if signal == "BUY":
            return {
                "signal": "BUY",  # ← CRITICAL: must be "signal" not "action"!
                "score": 5.0,  # Grid trading always full conviction
                "stop_loss": sl if sl > 0 else current_price * 0.98,  # ← Must be "stop_loss" not "stop_price"!
                "take_profit": tp if tp > 0 else current_price * 1.02,  # ← Must be "take_profit" not "target_price"!
                "current_price": current_price,
                "entry_reason": f"grid_cross_down_{metadata.get('grid_info', {}).get('total_buy_executions', 0)}",
                **metadata,
            }
        elif signal == "SELL":
            return {
                "signal": "SELL",
                "score": 5.0,
                "stop_loss": sl if sl > 0 else current_price * 0.98,  # ← Must be "stop_loss"!
                "take_profit": tp if tp > 0 else current_price * 1.02,  # ← Must be "take_profit"!
                "current_price": current_price,
                "entry_reason": f"grid_cross_up_{metadata.get('grid_info', {}).get('total_sell_executions', 0)}",
                **metadata,
            }
        else:  # HOLD or EMERGENCY_EXIT
            return {
                "signal": "HOLD",
                "score": 0.0,
                "reason": metadata.get("reason", "no_signal"),
                "current_price": current_price,
                **metadata,
            }

    def analyze(
        self,
        symbol: str,
        ohlcv: List[List[float]],
        timeframe: str = "15m",
    ) -> Tuple[str, float, float, Dict[str, Any]]:
        """
        Analyze market and determine grid action
        
        Returns:
            (signal, stop_price, target_price, metadata)
            signal: "BUY", "SELL", or "HOLD"
        """
        
        if len(ohlcv) < 200:
            return ("HOLD", 0.0, 0.0, {"reason": "insufficient_data"})
        
        # Get current price
        current_price = float(ohlcv[-1][4])  # Close
        
        # Get symbol-specific grid state
        grid_state = self._get_symbol_grid(symbol)
        
        # Update legacy attributes for backward compat
        if symbol == self.default_symbol:
            self.grids = grid_state.grids
            self.grid_lower = grid_state.grid_lower
            self.grid_upper = grid_state.grid_upper
            self.last_price = grid_state.last_price
            self.execution_history = grid_state.execution_history
        
        # Check if grid needs rebalancing (auto-adjust to price movement)
        rebalanced = self._check_and_rebalance_grid_for_symbol(symbol, current_price, grid_state)
        if rebalanced:
            # Grid was rebalanced, return HOLD this iteration
            return ("HOLD", 0.0, 0.0, {
                "reason": "grid_rebalanced",
                "current_price": current_price,
                "new_range": f"${grid_state.grid_lower:.0f} - ${grid_state.grid_upper:.0f}",
                "strategy_name": "grid_v1",
            })
        
        # ── REGIME DETECTION ──────────────────────────────────────────────
        highs  = [float(c[2]) for c in ohlcv]
        lows   = [float(c[3]) for c in ohlcv]
        closes = [float(c[4]) for c in ohlcv]

        regime_result = self._regime_detector.detect(highs, lows, closes)
        regime_label  = regime_result.regime          # "TREND" | "CHOP" | "HIGH_VOL" | "UNKNOWN"
        regime_adx    = regime_result.adx
        regime_er     = regime_result.er
        regime_atr_pct = regime_result.atr_pct

        # Watchdog için cache'e yaz (thread-safe değil ama GIL sayesinde sorunsuz)
        import time as _time
        self._regime_cache[symbol] = {
            "regime":   regime_label,
            "adx":      regime_adx,
            "er":       regime_er,
            "atr_pct":  regime_atr_pct,
            "updated_at": _time.time(),
        }
        # ── REGIME DETECTION END ──────────────────────────────────────────

        # CRITICAL: Check for grid crossings FIRST (before gates!)
        signal, crossing_metadata = self._check_grid_crossings_for_symbol(current_price, grid_state)
        
        # Update last_price for next iteration (MUST happen every time!)
        grid_state.last_price = current_price
        
        # If we got a signal from grid crossing, validate with gates
        if signal != "HOLD":
            # Calculate indicators for gate checks
            ind = Indicators.from_ohlcv(ohlcv)
            gates = self._check_gates_for_symbol(current_price, ind, grid_state)
            
            # If gates fail, block the signal but still return metadata
            if not gates["all_gates_pass"]:
                return ("HOLD", 0.0, 0.0, {
                    "reason": "signal_blocked_by_gates",
                    "would_be_signal": signal,
                    "current_price": current_price,
                    **gates
                })
            
            # Gates passed! Return the signal with crossing metadata

            # ── AKILLI BUY FİLTRESİ (3-4 adım ötesi düşünme) ─────────────
            if signal == "BUY":

                # 1. INVENTORİ LİMİTİ: Aynı yönde çok fazla pozisyon birikmiş mi?
                # Memory'deki grid seviyeleri (bot restart'ta sıfırlanabilir)
                # + execution_history'den net unsold hesabı
                max_consecutive_buys = int(os.getenv("GRID_MAX_CONSECUTIVE_BUYS", "3"))
                
                _inv_levels_mem = sum(1 for g in grid_state.grids if g.quantity > 1e-10) if hasattr(grid_state, "grids") else 0
                
                # execution_history'den net buy sayısı (daha güvenilir)
                _hist = grid_state.execution_history if hasattr(grid_state, "execution_history") else []
                _hist_buys  = sum(1 for e in _hist if hasattr(e, "action") and str(e.action).upper() == "BUY")
                _hist_sells = sum(1 for e in _hist if hasattr(e, "action") and str(e.action).upper() == "SELL")
                _net_unsold = max(_inv_levels_mem, _hist_buys - _hist_sells)
                
                if _net_unsold >= max_consecutive_buys:
                    logger.info(
                        "grid_buy_blocked_inventory_limit sym=%s net_unsold=%d max=%d "
                        "(mem_levels=%d hist_buys=%d hist_sells=%d)",
                        symbol, _net_unsold, max_consecutive_buys,
                        _inv_levels_mem, _hist_buys, _hist_sells,
                    )
                    return ("HOLD", 0.0, 0.0, {
                        "reason": f"grid_buy_blocked_inventory_limit({_net_unsold}>={max_consecutive_buys})",
                        "current_price": current_price,
                        "strategy_name": "grid_v1",
                        "regime": regime_label,
                        "adx": regime_adx,
                        "er": regime_er,
                    })

                # 2. MOMENTUM FİLTRESİ: Son N mum aşağıysa BUY'ı ertele
                momentum_lookback = int(os.getenv("GRID_MOMENTUM_LOOKBACK", "4"))
                momentum_filter_enabled = os.getenv("GRID_MOMENTUM_FILTER", "true").lower() == "true"
                if momentum_filter_enabled and len(ohlcv) >= momentum_lookback + 1:
                    _closes = [float(c[4]) for c in ohlcv[-(momentum_lookback + 1):]]
                    _bearish_candles = sum(1 for i in range(1, len(_closes)) if _closes[i] < _closes[i-1])
                    _all_bearish = _bearish_candles >= momentum_lookback  # tüm mumlar aşağı
                    
                    # ER (trend gücü) yüksekse ve momentum aşağıysa blokla
                    _er_threshold = float(os.getenv("GRID_MOMENTUM_ER_THRESHOLD", "0.4"))
                    _strong_downtrend = _all_bearish and (regime_er or 0) > _er_threshold
                    
                    if _strong_downtrend:
                        logger.info(
                            "grid_buy_blocked_momentum sym=%s bearish_candles=%d/%d er=%.2f er_threshold=%.2f",
                            symbol, _bearish_candles, momentum_lookback,
                            regime_er or 0, _er_threshold,
                        )
                        return ("HOLD", 0.0, 0.0, {
                            "reason": f"grid_buy_blocked_momentum(er={regime_er:.2f})" if regime_er else "grid_buy_blocked_momentum",
                            "current_price": current_price,
                            "strategy_name": "grid_v1",
                            "regime": regime_label,
                            "adx": regime_adx,
                            "er": regime_er,
                        })
            # ── AKILLI BUY FİLTRESİ SONU ─────────────────────────────────

            quantity = self._calculate_quantity(current_price)
            
            # ── CROSS-ASSET KORELASYONSİSTEMİK RİSK FİLTRESİ ────────────
            # BTC güçlü TREND'deyken altcoin BUY'larını engelle.
            # Neden: BTC/ETH/SOL korelasyonu kriz anında ~0.95-0.99'a çıkar.
            # BTC TREND aşağıysa altcoinler daha sert düşer (beta etkisi).
            # BTC TREND yukarıysa da risk var: tepe yakınında BUY açılır.
            # Bu filtre 19 Mart'taki zincirleme stop-loss'u önlerdi.
            _btc_corr_filter_enabled = os.getenv("GRID_BTC_TREND_FILTER", "true").lower() == "true"
            _btc_adx_threshold = float(os.getenv("GRID_BTC_TREND_ADX", "40"))
            _btc_er_threshold  = float(os.getenv("GRID_BTC_TREND_ER",  "0.4"))
            if (signal == "BUY"
                    and _btc_corr_filter_enabled
                    and symbol not in ("BTC/USDT", "BTC/USD", "BTCUSDT")):
                # BTC'nin güncel rejimini oku (strategy._regime_cache)
                _btc_regime = self._regime_cache.get("BTC/USDT") or self._regime_cache.get("BTCUSDT")
                if _btc_regime:
                    _btc_adx = float(_btc_regime.get("adx") or 0)
                    _btc_er  = float(_btc_regime.get("er")  or 0)
                    _btc_strong_trend = _btc_adx >= _btc_adx_threshold and _btc_er >= _btc_er_threshold
                    if _btc_strong_trend:
                        logger.info(
                            "grid_buy_blocked_btc_trend sym=%s btc_adx=%.1f btc_er=%.3f "
                            "(threshold: adx>=%.0f er>=%.2f)",
                            symbol, _btc_adx, _btc_er, _btc_adx_threshold, _btc_er_threshold,
                        )
                        return ("HOLD", 0.0, 0.0, {
                            "reason": f"grid_buy_blocked_btc_trend(btc_adx={_btc_adx:.1f},btc_er={_btc_er:.3f})",
                            "current_price": current_price,
                            "strategy_name": "grid_v1",
                            "regime": regime_label,
                            "adx": regime_adx,
                            "er": regime_er,
                        })
            # ── CROSS-ASSET FİLTRE SONU ──────────────────────────────────

            # ── REGIME GATE: TREND piyasada yeni BUY açma ──────────────
            # Trend'de grid BUY'ları genellikle zararlı:
            # fiyat düşmeye devam eder, tüm seviyeleri doldurur.
            # CHOP/RANGE/UNKNOWN'da grid tam verimli çalışır.
            if signal == "BUY" and regime_label == "TREND":
                trend_block_new_buys = os.getenv("GRID_BLOCK_BUY_IN_TREND", "true").lower() == "true"
                if trend_block_new_buys:
                    logger.info(
                        "grid_trend_buy_blocked symbol=%s regime=%s adx=%.1f er=%.2f",
                        symbol, regime_label,
                        regime_adx if regime_adx is not None else 0.0,
                        regime_er  if regime_er  is not None else 0.0,
                    )
                    return ("HOLD", 0.0, 0.0, {
                        "reason": f"grid_buy_blocked_trend(adx={regime_adx:.1f},er={regime_er:.2f})" if regime_adx else "grid_buy_blocked_trend",
                        "current_price": current_price,
                        "strategy_name": "grid_v1",
                        "regime": regime_label,
                        "adx": regime_adx,
                        "er": regime_er,
                        "atr_pct": regime_atr_pct,
                    })
            # ── REGIME GATE END ──────────────────────────────────────────

            metadata = {
                "signal": signal,
                "current_price": current_price,
                "quantity": quantity,
                "grid_info": self._get_grid_status_for_symbol(grid_state),
                "strategy_name": "grid_v1",
                # Regime bilgileri — scanner log ve dashboard için
                "regime": regime_label,
                "adx": regime_adx,
                "er": regime_er,
                "atr_pct": regime_atr_pct,
                **gates
            }
            
            # Add crossing metadata (grid_price, action) if available
            if crossing_metadata:
                metadata["crossing_metadata"] = crossing_metadata

            # Profit check for SELL signals
            if crossing_metadata and crossing_metadata.get("action") == "SELL":
                sell_only_profitable = os.getenv("GRID_SELL_ONLY_PROFITABLE", "true").lower() == "true"

                if sell_only_profitable:
                    # ── Dinamik minimum kâr eşiği (ATR bazlı) ──────────────
                    # Sabit %0.30 yerine piyasa volatilitesine göre ölçeklenir.
                    # Formül: tp = max(fee_buffer, atr_pct * factor)
                    #   fee_buffer = BUY+SELL toplam komisyon × güvenlik çarpanı
                    #   atr_pct    = son 20 mumun (high-low)/close ortalaması
                    #   factor     = ATR'nin bu kadar kısmı hedeflenir
                    fee_buffer   = float(os.getenv("GRID_FEE_BUFFER_PCT", "0.004"))  # %0.40
                    atr_factor   = float(os.getenv("GRID_ATR_TP_FACTOR", "0.5"))
                    static_floor = float(os.getenv("GRID_MIN_PROFIT_TO_SELL", "0.003"))  # fallback

                    # ATR hesapla — OHLCV varsa kullan, yoksa statik floor
                    dynamic_tp = static_floor
                    if ohlcv and len(ohlcv) >= 5:
                        try:
                            recent = ohlcv[-20:]
                            atr_vals = []
                            for candle in recent:
                                h = float(candle[2])
                                l = float(candle[3])
                                c = float(candle[4])
                                if c > 0:
                                    atr_vals.append((h - l) / c)
                            if atr_vals:
                                atr_pct = sum(atr_vals) / len(atr_vals)
                                dynamic_tp = max(fee_buffer, atr_pct * atr_factor)
                                # Üst sınır: aşırı volatil piyasada çıkışı zorlaştırma
                                dynamic_tp = min(dynamic_tp, 0.015)  # max %1.5
                        except Exception:
                            dynamic_tp = static_floor

                    # Grid seviyesinin KENDİ kârını hesapla
                    grid_buy_price = float(crossing_metadata.get("grid_price", 0))

                    if grid_buy_price > 0:
                        grid_pnl_pct = (current_price - grid_buy_price) / grid_buy_price

                        if (grid_pnl_pct < dynamic_tp - 1e-9) and grid_pnl_pct > -0.15:
                            logger.info(
                                "⏸️ GRID SELL BLOCKED: %s grid_pnl=%.3f%% < min %.3f%% "
                                "(grid_buy=%.2f current=%.2f atr_based=%s)",
                                symbol,
                                grid_pnl_pct * 100,
                                dynamic_tp * 100,
                                grid_buy_price,
                                current_price,
                                f"{dynamic_tp*100:.3f}%",
                            )
                            return ("HOLD", 0.0, 0.0, {
                                "reason": f"grid_sell_blocked_unprofitable_{grid_pnl_pct:.2%}",
                                "current_price": current_price,
                                "strategy_name": "grid_v1",
                            })

                        logger.info(
                            "✅ GRID SELL ALLOWED: %s grid_pnl=%.2f%% >= min %.3f%% "
                            "(grid_buy=%.2f current=%.2f atr_tp=%.3f%%)",
                            symbol,
                            grid_pnl_pct * 100,
                            dynamic_tp * 100,
                            grid_buy_price,
                            current_price,
                            dynamic_tp * 100,
                        )

            return (signal, 0.0, 0.0, metadata)
        
        # No grid crossing, just HOLD — regime bilgisini yine de döndür
        return ("HOLD", 0.0, 0.0, {
            "reason": "no_grid_crossing",
            "current_price": current_price,
            "strategy_name": "grid_v1",
            "regime": regime_label,
            "adx": regime_adx,
            "er": regime_er,
            "atr_pct": regime_atr_pct,
        })
    
    def _check_gates_for_symbol(self, current_price: float, ind: Dict[str, Any], grid_state: SymbolGridState) -> Dict[str, Any]:
        """Check basic gate conditions for specific symbol"""
        
        # Gate 1: Price in range (use symbol's grid range!)
        price_in_range = grid_state.grid_lower <= current_price <= grid_state.grid_upper
        
        # Gate 2: ATR reasonable
        atr_ok = False
        if ind.get("atr") and current_price:
            atr_pct = ind["atr"] / current_price
            atr_ok = self.min_atr_pct <= atr_pct <= self.max_atr_pct
        
        # Gate 3: Volume OK (simplified - always True for grid trading)
        volume_ok = True
        
        gates_passed = sum([price_in_range, atr_ok, volume_ok])
        all_gates_pass = gates_passed >= 2  # At least 2/3 gates
        
        return {
            "price_in_range": price_in_range,
            "atr_ok": atr_ok,
            "volume_ok": volume_ok,
            "gates_passed": gates_passed,
            "all_gates_pass": all_gates_pass,
        }
    
    def _check_gates(self, current_price: float, ind: Dict[str, Any]) -> Dict[str, Any]:
        """Legacy wrapper - uses default symbol's grid"""
        grid_state = self._get_symbol_grid(self.default_symbol)
        return self._check_gates_for_symbol(current_price, ind, grid_state)
    
    def _is_emergency_condition(self, current_price: float) -> bool:
        """Check if price has broken out of range significantly"""
        range_size = self.grid_upper - self.grid_lower
        emergency_threshold = range_size * self.emergency_stop_pct
        
        # Price way below lower bound
        if current_price < (self.grid_lower - emergency_threshold):
            return True
        
        # Price way above upper bound
        if current_price > (self.grid_upper + emergency_threshold):
            return True
        
        return False
    
    def _check_grid_crossings_for_symbol(self, current_price: float, grid_state: SymbolGridState):
        if grid_state.last_price is None:
            return ("HOLD", None)

        for grid in grid_state.grids:
            grid_price = grid.price
            
            # 1. YUKARI KIRILIM: Fiyat grid seviyesinin ÜSTÜNE çıktı
            # Mantık: Fiyat grid'i yukarı geçtiyse, elinde coin varsa SELL yap
            if grid_state.last_price < grid_price <= current_price:
                # Eğer bu grid seviyesinde pozisyon (coin) varsa sat
                if grid.quantity > 0:
                    return ("SELL", {
                        "grid_price": grid_price,
                        "quantity": grid.quantity,
                        "action": "SELL",
                        "reason": "grid_crossing_up"
                    })
                # quantity=0 ama execution_history'de bu fiyat civarında BUY var
                # (restart sonrası restore edilemeyen durumlar için)
                elif grid_state.execution_history:
                    _unmatched_buys = [
                        e for e in grid_state.execution_history
                        if str(e.action).upper() == "BUY"
                        and abs(e.grid_price - grid_price) < (grid_state.grid_upper - grid_state.grid_lower) / len(grid_state.grids) * 1.5
                    ]
                    if _unmatched_buys:
                        _qty = _unmatched_buys[-1].quantity
                        return ("SELL", {
                            "grid_price": grid_price,
                            "quantity": _qty,
                            "action": "SELL",
                            "reason": "grid_crossing_up_history_match"
                        })
            
            # 2. AŞAĞI KIRILIM: Fiyat grid seviyesinin ALTINA düştü
            # Mantık: Fiyat grid'i aşağı geçtiyse, elinde coin yoksa BUY yap
            elif grid_state.last_price > grid_price >= current_price:
                # Eğer bu grid seviyesinde pozisyon yoksa al
                if grid.quantity == 0:
                    quantity = self._calculate_quantity(current_price)
                    return ("BUY", {
                        "grid_price": grid_price,
                        "quantity": quantity,
                        "action": "BUY",
                        "reason": "grid_crossing_down"
                    })

        return ("HOLD", None)
    
    def _check_grid_crossings(self, current_price: float) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        Legacy wrapper - uses default symbol's grid
        
        Returns: (signal, metadata) tuple
        """
        grid_state = self._get_symbol_grid(self.default_symbol)
        return self._check_grid_crossings_for_symbol(current_price, grid_state)
    
    def _calculate_quantity(self, current_price: float) -> float:
        """Calculate position size based on grid capital"""
        return self.capital_per_grid / current_price
    
    def _get_grid_status_for_symbol(self, grid_state: SymbolGridState) -> Dict[str, Any]:
        """Get current grid status for specific symbol"""
        total_inventory = sum(g.quantity for g in grid_state.grids)
        total_buys = sum(g.buy_count for g in grid_state.grids)
        total_sells = sum(g.sell_count for g in grid_state.grids)
        
        # Find grids with inventory
        active_grids = [
            {"price": g.price, "quantity": g.quantity} 
            for g in grid_state.grids 
            if g.quantity > 0
        ]
        
        return {
            "total_inventory": total_inventory,
            "total_buy_executions": total_buys,
            "total_sell_executions": total_sells,
            "active_grid_count": len(active_grids),
            "active_grids": active_grids[:5],  # Show first 5
        }
    
    def _get_grid_status(self) -> Dict[str, Any]:
        """Legacy wrapper - uses default symbol's grid"""
        grid_state = self._get_symbol_grid(self.default_symbol)
        return self._get_grid_status_for_symbol(grid_state)
    
    def reset_grids(self) -> None:
        """Reset all grid state (for backtesting)"""
        self._initialize_grids()
        self.last_price = None
        self.execution_history = []
    
    # ------------------------------------------------------------------
    # Regime cache (watchdog erişimi için)
    # ------------------------------------------------------------------

    def get_cached_regime(self, symbol: str, max_age_s: float = 120.0) -> dict | None:
        """
        Son hesaplanan regime bilgisini döndür.
        max_age_s saniyeden eskiyse None döner (cache expired).
        Watchdog ve diğer modüller bu metodu kullanır.
        """
        import time as _time
        cached = self._regime_cache.get(symbol)
        if cached is None:
            return None
        age = _time.time() - float(cached.get("updated_at", 0))
        if age > max_age_s:
            return None
        return dict(cached)

    # ------------------------------------------------------------------
    # Grid state serialization (DB persist / restore)
    # ------------------------------------------------------------------

    def grid_state_to_dict(self, symbol: str) -> dict:
        """Serialize grid state for a symbol to a plain dict (JSON-safe)."""
        grid_state = self._get_symbol_grid(symbol)
        return {
            "symbol": symbol,
            "grid_lower": grid_state.grid_lower,
            "grid_upper": grid_state.grid_upper,
            "last_price": grid_state.last_price,
            "grids": [
                {
                    "price": g.price,
                    "quantity": g.quantity,
                    "buy_count": g.buy_count,
                    "sell_count": g.sell_count,
                }
                for g in grid_state.grids
            ],
        }

    def restore_grid_state_from_dict(self, symbol: str, data: dict) -> bool:
        """
        Restore grid state from a persisted dict.
        Returns True on success, False on failure.
        Grid levels with quantity > 0 are restored so open positions survive restart.
        """
        try:
            if symbol not in self._symbol_grids:
                self._init_symbol_grid(symbol)

            grid_state = self._symbol_grids[symbol]
            grid_state.last_price = data.get("last_price")

            # Rebuild grids from saved data, preserving inventory
            saved_grids = data.get("grids", [])
            if not saved_grids:
                return False

            # Match saved grid levels to current grids by price (1 USDT tolerance)
            restored = 0
            # Grid spacing hesapla - dinamik tolerans için
            _grid_spacing = 0.0
            if len(grid_state.grids) > 1:
                _grid_spacing = abs(grid_state.grids[1].price - grid_state.grids[0].price)
            _tolerance = max(1.0, _grid_spacing * 0.6)  # En az 1, en fazla grid spacing'in %60'ı

            for saved in saved_grids:
                saved_price = float(saved["price"])
                saved_qty = float(saved.get("quantity", 0.0))
                if saved_qty <= 0:
                    continue  # nothing to restore for empty levels
                # En yakın grid seviyesini bul (sabit tolerans yerine dinamik)
                best_match = None
                best_dist = float("inf")
                for g in grid_state.grids:
                    dist = abs(g.price - saved_price)
                    if dist < best_dist:
                        best_dist = dist
                        best_match = g
                if best_match is not None and best_dist < _tolerance:
                    best_match.quantity = saved_qty
                    best_match.buy_count = int(saved.get("buy_count", 0))
                    best_match.sell_count = int(saved.get("sell_count", 0))
                    restored += 1

            # Restore sırasında execution_history'yi yeniden oluştur
            # Her restore edilen seviye = 1 net açık BUY sayılır
            # Bu sayede inventory limiti restart sonrası da çalışır
            if restored > 0:
                grid_state.execution_history = [
                    GridExecution(
                        grid_price=g.price,
                        action="BUY",
                        quantity=g.quantity,
                        timestamp_ms=0,
                    )
                    for g in grid_state.grids if g.quantity > 1e-10
                ]
            
            logger.info(
                "grid_state_restored symbol=%s levels_with_inventory=%s",
                symbol, restored,
            )
            return True
        except Exception as e:
            logger.warning("grid_state_restore_failed symbol=%s err=%s", symbol, str(e)[:200])
            return False

    def _record_grid_execution(
        self, 
        symbol: str, 
        action: str, 
        grid_price: float, 
        quantity: float,
        trade_success: bool = True
    ) -> None:
        """
        Record grid execution AFTER successful trade
        
        CRITICAL: Only call this AFTER trade is confirmed executed!
        This prevents phantom trades when trades are blocked.
        
        Args:
            symbol: Trading symbol (e.g., "BTC/USDT")
            action: "BUY" or "SELL"
            grid_price: Price of grid level
            quantity: Amount traded
            trade_success: Whether trade actually executed (default True)
        
        Example usage in main.py:
            # After getting signal from strategy
            signal, sl, tp, metadata = strategy.analyze(...)
            
            if signal == "BUY":
                # Try to execute trade
                trade_result = trader.place_order(...)
                
                # Only update grid state if successful
                if trade_result and trade_result.status == 'filled':
                    crossing_meta = metadata.get('crossing_metadata', {})
                    strategy._record_grid_execution(
                        symbol=symbol,
                        action=crossing_meta.get('action', 'BUY'),
                        grid_price=crossing_meta.get('grid_price', current_price),
                        quantity=crossing_meta.get('quantity', 0),
                        trade_success=True
                    )
        """
        if not trade_success:
            # Trade failed/blocked - DO NOT update state!
            return
        
        grid_state = self._get_symbol_grid(symbol)
        
        # Find the grid level (with small tolerance for float comparison)
        grid_level = None
        for grid in grid_state.grids:
            if abs(grid.price - grid_price) < 1.0:  # 1 USDT tolerance
                grid_level = grid
                break
        
        if grid_level is None:
            # Grid level not found - this shouldn't happen but handle gracefully
            return
        
        # NOW update state (AFTER confirmed execution)
        grid_state.execution_history.append(GridExecution(
            grid_price=grid_price,
            action=action,
            quantity=quantity,
            timestamp_ms=int(pd.Timestamp.now().timestamp() * 1000)
        ))
        
        if action == "BUY":
            grid_level.buy_count += 1
            grid_level.quantity += quantity
        elif action == "SELL":
            grid_level.sell_count += 1
            grid_level.quantity = 0.0  # Sold all at this grid
    
    
    def _check_and_rebalance_grid_for_symbol(self, symbol: str, current_price: float, grid_state: SymbolGridState) -> bool:
        """
        Grid rebalance mantığı — iki mod:

        1. HARD rebalance (inventory sıfır): grid seviyelerini sıfırdan kur,
           mevcut pozisyon yok, tam temiz başlangıç.

        2. SOFT rebalance (inventory var): grid sınırlarını güncelle ama
           mevcut envanterin bulunduğu seviyeleri koru.
           Yeni BUY/SELL sinyalleri yeni aralığa göre üretilir.
           Açık pozisyonlar watchdog + crossing mantığıyla kapatılır.

        Rebalance tetik eşiği: fiyat aralık dışına çıktığında
        (GRID_REBALANCE_TRIGGER_PCT, default %0 = herhangi bir dışarı çıkış).
        """
        import logging as _log
        _logger = _log.getLogger(__name__)

        trigger_pct = float(os.getenv("GRID_REBALANCE_TRIGGER_PCT", "0.0"))
        range_size  = grid_state.grid_upper - grid_state.grid_lower
        buffer      = range_size * trigger_pct

        outside_range = (
            current_price > (grid_state.grid_upper + buffer) or
            current_price < (grid_state.grid_lower - buffer)
        )

        if not outside_range:
            return False

        # ── Yeni aralığı hesapla ──────────────────────────────────────────
        base_asset    = symbol.split("/")[0] if "/" in symbol else symbol
        sym_width_key = f"{base_asset}_GRID_WIDTH_PCT"
        grid_width_pct = float(os.getenv(sym_width_key) or os.getenv("GRID_WIDTH_PCT") or "0.08")
        new_lower = current_price * (1 - grid_width_pct)
        new_upper = current_price * (1 + grid_width_pct)

        total_inventory = sum(g.quantity for g in grid_state.grids if g.quantity > 1e-10)

        if total_inventory < 1e-8:
            # ── HARD rebalance: inventory yok, tamamen yeniden kur ────────
            step = (new_upper - new_lower) / self.grid_count
            grid_state.grids = [
                GridLevel(price=new_lower + i * step)
                for i in range(self.grid_count + 1)
            ]
            grid_state.last_price   = None
            grid_state.needs_rebalance = False
            _logger.info(
                "grid_hard_rebalance sym=%s px=%.2f new_range=[%.2f,%.2f]",
                symbol, current_price, new_lower, new_upper,
            )
        else:
            # ── SOFT rebalance: inventory var, sadece sınırları güncelle ──
            # Envanter taşıyan seviyeleri koru, boş seviyeleri yeniden dağıt.
            step = (new_upper - new_lower) / self.grid_count
            new_prices = set(round(new_lower + i * step, 8) for i in range(self.grid_count + 1))

            # Mevcut envanterleri koru
            kept_levels = [g for g in grid_state.grids if g.quantity > 1e-10]

            # Yeni boş seviyeleri ekle (envanterlilerle çakışmayan)
            kept_prices = {round(g.price, 8) for g in kept_levels}
            new_empty   = [
                GridLevel(price=p)
                for p in sorted(new_prices - kept_prices)
            ]

            grid_state.grids = sorted(kept_levels + new_empty, key=lambda g: g.price)
            grid_state.last_price   = None
            grid_state.needs_rebalance = False
            _logger.info(
                "grid_soft_rebalance sym=%s px=%.2f new_range=[%.2f,%.2f] "
                "kept_inventory_levels=%d new_empty_levels=%d",
                symbol, current_price, new_lower, new_upper,
                len(kept_levels), len(new_empty),
            )

        # Ortak güncelleme
        grid_state.grid_lower = new_lower
        grid_state.grid_upper = new_upper

        if symbol == self.default_symbol:
            self.grid_lower = new_lower
            self.grid_upper = new_upper
            self.grids      = grid_state.grids

        return True
    
    def check_and_rebalance_grid(self, current_price: float) -> bool:
        """Legacy wrapper - uses default symbol's grid"""
        grid_state = self._get_symbol_grid(self.default_symbol)
        return self._check_and_rebalance_grid_for_symbol(self.default_symbol, current_price, grid_state)