from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel


class Sleeve(str, Enum):
    SHORT = "short"
    MEDIUM = "medium"
    LONG = "long"


class Regime(str, Enum):
    BULLISH = "BULLISH"
    NEUTRAL_UP = "NEUTRAL_UP"
    RANGE = "RANGE"
    BEARISH = "BEARISH"
    PANIC = "PANIC"


class Decision(str, Enum):
    BUY = "BUY"
    HOLD = "HOLD"
    SELL = "SELL"


class SignalData(BaseModel):
    symbol: str
    sleeve: Sleeve
    strategy_name: str
    signal: str
    score: float
    buy_threshold: float
    gap_to_threshold: float
    regime: str
    dir_1h: Optional[str] = None
    atr_pct: Optional[float] = None
    price: float
    reason: str
    gate_status: Dict[str, Any]
    hold_fail_reasons: list[str]
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trail_atr: Optional[float] = None
    risk_multiplier: float = 1.0
