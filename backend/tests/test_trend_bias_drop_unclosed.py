# File: backend/tests/test_trend_bias_drop_unclosed.py
from backend.core.ohlcv_utils import drop_unclosed_last_candle

# helper timestamps (ms)
ts_09_00 = 9 * 60 * 60 * 1000
ts_10_00 = 10 * 60 * 60 * 1000

def test_drop_unclosed_last_candle():
    tf = "1h"
    # last candle starts at 10:00, should close at 11:00
    ohlcv = [
        [ts_09_00, 1, 1, 1, 1, 1],
        [ts_10_00, 1, 1, 1, 1, 1],
    ]
    now_ms = ts_10_00 + 30 * 60 * 1000  # 10:30 -> unclosed
    out = drop_unclosed_last_candle(ohlcv, tf, now_ms)
    assert len(out) == 1

    now_ms2 = ts_10_00 + 60 * 60 * 1000  # 11:00 -> closed
    out2 = drop_unclosed_last_candle(ohlcv, tf, now_ms2)
    assert len(out2) == 2