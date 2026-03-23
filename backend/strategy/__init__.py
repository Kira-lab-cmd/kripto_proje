from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_legacy_strategy_module():
    module_name = "backend._legacy_strategy_module"
    module = sys.modules.get(module_name)
    if module is not None:
        return module

    legacy_path = Path(__file__).resolve().parents[1] / "strategy.py"
    spec = importlib.util.spec_from_file_location(module_name, legacy_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load legacy strategy module from {legacy_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_legacy = _load_legacy_strategy_module()

TradingStrategy = _legacy.TradingStrategy
GATE_STATUS_KEYS = _legacy.GATE_STATUS_KEYS
ALL_GATE_STATUS_KEYS = getattr(_legacy, "ALL_GATE_STATUS_KEYS", ())

__all__ = ["ALL_GATE_STATUS_KEYS", "GATE_STATUS_KEYS", "TradingStrategy"]
