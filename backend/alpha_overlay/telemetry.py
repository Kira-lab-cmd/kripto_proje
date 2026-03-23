from __future__ import annotations

import json
import logging
from typing import Any


logger = logging.getLogger(__name__)


def log_trade_close(payload: dict[str, Any]) -> None:
    try:
        logger.info("overlay_trade_close %s", json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
    except Exception:
        logger.exception("overlay telemetry failed")
