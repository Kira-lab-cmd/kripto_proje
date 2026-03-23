from __future__ import annotations

import os
from dataclasses import dataclass


def _env_on(name: str) -> bool:
    return str(os.getenv(name, "")).strip() == "1"


@dataclass(frozen=True)
class OverlayFeatureFlags:
    overlay_enabled: bool
    signal_tap: bool
    portfolio_risk: bool
    telemetry: bool

    @classmethod
    def from_env(cls) -> "OverlayFeatureFlags":
        overlay_enabled = _env_on("FF_OVERLAY_ENABLED")
        return cls(
            overlay_enabled=overlay_enabled,
            signal_tap=overlay_enabled and _env_on("FF_SIGNAL_TAP"),
            portfolio_risk=overlay_enabled and _env_on("FF_PORTFOLIO_RISK"),
            telemetry=overlay_enabled and _env_on("FF_TELEMETRY"),
        )
