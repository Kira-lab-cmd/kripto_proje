# backend/re_entry_guard.py

import time
import os
import logging

logger = logging.getLogger(__name__)

class ReEntryGuard:
    """
    Prevents buying back immediately after selling, especially at loss.
    Critical for avoiding whipsaw patterns.
    """
    
    def __init__(self):
        self.recent_sells = {}  # {symbol: sell_info}
        self.base_cooldown = int(os.getenv("RE_ENTRY_COOLDOWN_SECONDS", "600"))
        self.loss_mult = float(os.getenv("RE_ENTRY_LOSS_COOLDOWN_MULT", "2.0"))
        self.prevent_higher = os.getenv("RE_ENTRY_PREVENT_HIGHER_BUY", "true").lower() == "true"
        self.max_increase_pct = float(os.getenv("RE_ENTRY_MAX_PRICE_INCREASE_PCT", "0.005"))
    
    def mark_sell(self, symbol: str, sell_price: float, entry_price: float, reason: str = ""):
        """Record a sell for re-entry tracking"""
        pnl_pct = (sell_price - entry_price) / entry_price if entry_price > 0 else 0
        was_loss = pnl_pct < 0
        
        self.recent_sells[symbol] = {
            'timestamp': time.time(),
            'sell_price': sell_price,
            'entry_price': entry_price,
            'pnl_pct': pnl_pct,
            'was_loss': was_loss,
            'reason': reason
        }
        
        if was_loss:
            logger.warning(
                f"🔴 SELL AT LOSS: {symbol} @ ${sell_price:.2f} "
                f"(entry: ${entry_price:.2f}, PnL: {pnl_pct:.2%}) - {reason}"
            )
        else:
            logger.info(
                f"🟢 SELL AT PROFIT: {symbol} @ ${sell_price:.2f} "
                f"(entry: ${entry_price:.2f}, PnL: {pnl_pct:.2%})"
            )
    
    def can_buy(self, symbol: str, buy_price: float) -> tuple[bool, str]:
        """
        Check if buying is allowed
        Returns: (can_buy: bool, reason: str)
        """
        if symbol not in self.recent_sells:
            return (True, "no_recent_sell")
        
        sell_info = self.recent_sells[symbol]
        elapsed = time.time() - sell_info['timestamp']
        
        # Calculate required cooldown
        required_cooldown = self.base_cooldown
        if sell_info['was_loss']:
            required_cooldown *= self.loss_mult
        
        # Check 1: Cooldown period
        if elapsed < required_cooldown:
            return (
                False,
                f"cooldown_{int(elapsed)}s_of_{int(required_cooldown)}s"
            )
        
        # Check 2: Prevent buying higher than sell (if sold at loss)
        if self.prevent_higher and sell_info['was_loss']:
            max_allowed_price = sell_info['sell_price'] * (1 + self.max_increase_pct)
            
            if buy_price > max_allowed_price:
                return (
                    False,
                    f"price_too_high_${buy_price:.2f}>max_${max_allowed_price:.2f}"
                )
        
        # Passed all checks - remove from tracking
        logger.info(
            f"✅ RE-ENTRY ALLOWED: {symbol} after {elapsed:.0f}s "
            f"(sold @ ${sell_info['sell_price']:.2f}, buying @ ${buy_price:.2f})"
        )
        del self.recent_sells[symbol]
        return (True, "re_entry_allowed")
    
    def get_status(self, symbol: str) -> dict:
        """Get current re-entry status for a symbol"""
        if symbol not in self.recent_sells:
            return {"blocked": False}
        
        sell_info = self.recent_sells[symbol]
        elapsed = time.time() - sell_info['timestamp']
        required = self.base_cooldown * (self.loss_mult if sell_info['was_loss'] else 1)
        
        return {
            "blocked": True,
            "elapsed_seconds": int(elapsed),
            "required_seconds": int(required),
            "remaining_seconds": int(max(0, required - elapsed)),
            "was_loss": sell_info['was_loss'],
            "sell_price": sell_info['sell_price'],
            "pnl_pct": sell_info['pnl_pct']
        }