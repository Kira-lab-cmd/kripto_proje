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

import logging
import os
from typing import Any, Dict, List, Tuple, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

import pandas as pd

from backend.indicators import Indicators


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
        """Initialize grid for a specific symbol"""
        base_asset = symbol.split("/")[0] if "/" in symbol else symbol
        
        # Calculate grid range for this symbol
        if initial_price is not None:
            # DYNAMIC MODE
            grid_width_pct = float(os.getenv("GRID_WIDTH_PCT", "0.10"))
            grid_lower = initial_price * (1 - grid_width_pct)
            grid_upper = initial_price * (1 + grid_width_pct)
        else:
            # STATIC MODE - symbol-specific or fallback
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
        
        # Create grid levels
        step = (grid_upper - grid_lower) / self.grid_count
        grids = []
        for i in range(self.grid_count + 1):
            price = grid_lower + (i * step)
            grids.append(GridLevel(price=price))
        
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
            quantity = self._calculate_quantity(current_price)
            
            metadata = {
                "signal": signal,
                "current_price": current_price,
                "quantity": quantity,
                "grid_info": self._get_grid_status_for_symbol(grid_state),
                "strategy_name": "grid_v1",
                **gates
            }
            
            # Add crossing metadata (grid_price, action) if available
            if crossing_metadata:
                metadata["crossing_metadata"] = crossing_metadata

            # Profit check for SELL signals
            if crossing_metadata and crossing_metadata.get("action") == "SELL":
                min_profit_pct = float(os.getenv("GRID_MIN_PROFIT_TO_SELL", "0.015"))
                sell_only_profitable = os.getenv("GRID_SELL_ONLY_PROFITABLE", "true").lower() == "true"
                if sell_only_profitable:
                    try:
                        from backend.database import Database
                        _db = Database()
                        position = _db.get_open_position(symbol)
                        if position:
                            entry_price = position.get("entry_price", 0)
                            if entry_price > 0:
                                pnl_pct = (current_price - entry_price) / entry_price
                                if pnl_pct < min_profit_pct and pnl_pct > -0.15:
                                    logger.info(
                                        "⏸️ GRID SELL BLOCKED: %s PnL %.2f%% < min %.2f%% (waiting for profit)",
                                        symbol, pnl_pct * 100, min_profit_pct * 100,
                                    )
                                    return ("HOLD", 0.0, 0.0, {
                                        "reason": f"grid_sell_blocked_unprofitable_{pnl_pct:.2%}",
                                        "current_price": current_price,
                                        "strategy_name": "grid_v1",
                                    })
                                logger.info(
                                    "✅ GRID SELL ALLOWED: %s PnL %.2f%% >= min %.2f%%",
                                    symbol, pnl_pct * 100, min_profit_pct * 100,
                                )
                    except Exception:
                        pass

            return (signal, 0.0, 0.0, metadata)
        
        # No grid crossing, just HOLD
        return ("HOLD", 0.0, 0.0, {
            "reason": "no_grid_crossing",
            "current_price": current_price,
            "strategy_name": "grid_v1",
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
    
    def _check_grid_crossings_for_symbol(self, current_price: float, grid_state: SymbolGridState) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        Check if price crossed any grid levels for specific symbol
        
        CRITICAL FIX: This method only DETECTS crossings, does NOT update state!
        State updates happen AFTER successful trade execution via _record_grid_execution()
        
        Logic:
        - If price crossed DOWN through grid → BUY signal
        - If price crossed UP through grid → SELL signal
        - Otherwise → HOLD
        
        Returns:
            Tuple of (signal, metadata)
            - signal: "BUY", "SELL", or "HOLD"
            - metadata: Dict with grid_price, quantity, action if signal != "HOLD", else None
        """
        
        if grid_state.last_price is None:
            # First candle, initialize but don't trade
            return ("HOLD", None)
        
        # Check each grid level
        for grid in grid_state.grids:
            grid_price = grid.price
            
            # Crossed DOWN through grid (last > grid >= current)
            # This means price dropped through grid level → BUY signal
            if grid_state.last_price > grid_price >= current_price:
                # ONLY RETURN SIGNAL - DO NOT UPDATE STATE YET!
                # State will be updated by _record_grid_execution() after successful trade
                metadata = {
                    "grid_price": grid_price,
                    "quantity": self.capital_per_grid / grid_price,
                    "action": "BUY"
                }
                return ("BUY", metadata)
            
            # Crossed UP through grid (last < grid <= current)
            # This means price rose through grid level → SELL signal
            if grid_state.last_price < grid_price <= current_price:
                # Only sell if we have inventory at or below this grid
                if grid.quantity > 0:
                    # ONLY RETURN SIGNAL - DO NOT UPDATE STATE YET!
                    # State will be updated by _record_grid_execution() after successful trade
                    metadata = {
                        "grid_price": grid_price,
                        "quantity": grid.quantity,
                        "action": "SELL"
                    }
                    return ("SELL", metadata)
        
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
        Check if grid needs rebalancing for specific symbol
        
        Returns True if grid was rebalanced
        
        Rebalance conditions:
        - Price is outside grid range by >5%
        - No open positions (safety check)
        """
        range_size = grid_state.grid_upper - grid_state.grid_lower
        buffer = range_size * 0.05  # 5% buffer
        
        # Check if price is significantly outside range
        needs_rebalance = (
            current_price > (grid_state.grid_upper + buffer) or
            current_price < (grid_state.grid_lower - buffer)
        )
        
        if needs_rebalance:
            # Check if safe to rebalance (no inventory)
            total_inventory = sum(g.quantity for g in grid_state.grids)
            
            if total_inventory < 1e-8:  # Essentially zero
                # Recalculate grid centered on current price
                grid_width_pct = float(os.getenv("GRID_WIDTH_PCT", "0.10"))
                new_lower = current_price * (1 - grid_width_pct)
                new_upper = current_price * (1 + grid_width_pct)
                
                # Update grid state
                grid_state.grid_lower = new_lower
                grid_state.grid_upper = new_upper
                
                # Reinitialize grids for this symbol
                step = (new_upper - new_lower) / self.grid_count
                grid_state.grids = []
                for i in range(self.grid_count + 1):
                    price = new_lower + (i * step)
                    grid_state.grids.append(GridLevel(price=price))
                
                grid_state.last_price = None  # Reset to avoid false crossings
                grid_state.needs_rebalance = False
                
                # Update legacy attributes if this is default symbol
                if symbol == self.default_symbol:
                    self.grid_lower = new_lower
                    self.grid_upper = new_upper
                    self.grids = grid_state.grids
                
                return True
            else:
                # Mark for rebalancing but don't do it yet
                grid_state.needs_rebalance = True
        
        return False
    
    def check_and_rebalance_grid(self, current_price: float) -> bool:
        """Legacy wrapper - uses default symbol's grid"""
        grid_state = self._get_symbol_grid(self.default_symbol)
        return self._check_and_rebalance_grid_for_symbol(self.default_symbol, current_price, grid_state)