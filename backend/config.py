# File: backend/config.py
from __future__ import annotations

from pathlib import Path
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


BACKEND_DIR = Path(__file__).resolve().parent
REPO_ROOT = BACKEND_DIR.parent


def _resolve_runtime_path(raw_path: str) -> Path:
    candidate = Path(str(raw_path or "").strip() or ".").expanduser()
    if candidate.is_absolute():
        return candidate.resolve()

    # Preserve current behavior for plain filenames while also supporting
    # repo-root relative examples like "backend/trading_bot.db".
    anchor = BACKEND_DIR if len(candidate.parts) == 1 else REPO_ROOT
    return (anchor / candidate).resolve()


class Settings(BaseSettings):
    """
    Central config surface.
    - Reads from backend/.env
    - Keeps existing uppercase fields for current runtime
    - Also supports lower-case attribute access for new modules
    """

    model_config = SettingsConfigDict(
        env_file=BACKEND_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        return init_settings, dotenv_settings, env_settings, file_secret_settings

    # App
    APP_NAME: str = "AI Crypto Trading Bot"
    APP_ENV: str = Field(default="development")
    ENV: str = Field(default="dev", description="dev|prod")
    DEBUG: bool = Field(default=False)
    LOG_LEVEL: str = Field(default="INFO")
    TZ: str = Field(default="Europe/Istanbul")

    # CORS
    CORS_ORIGINS: List[str] = Field(default_factory=lambda: ["http://localhost:5173"])

    # Trading mode
    TEST_MODE: bool = Field(default=True)
    PAPER_TRADING: bool = Field(default=True)
    TELEGRAM_ENABLED: bool = Field(default=True)

    # Databases
    DB_PATH: str = Field(default="trading_bot.db")
    RESEARCH_DB_PATH: str = Field(default="research.db")

    # Exchange / universe
    EXCHANGE_ID: str = Field(default="binance")
    QUOTE_ASSET: str = Field(default="USDT")
    SHORT_UNIVERSE: str = Field(default="SUI/USDT,NEAR/USDT,AVAX/USDT")
    MEDIUM_UNIVERSE: str = Field(default="LINK/USDT,AAVE/USDT,RNDR/USDT")
    LONG_UNIVERSE: str = Field(default="BTC/USDT,ETH/USDT")
    SHORT_TIMEFRAME: str = Field(default="5m")
    MEDIUM_TIMEFRAME: str = Field(default="15m")
    LONG_TIMEFRAME: str = Field(default="1h")

    # Capital / risk
    TOTAL_CAPITAL_USDT: float = Field(default=1000.0, ge=0.0)
    RISK_PER_TRADE: float = Field(default=0.01, ge=0.0, le=1.0)
    BASE_RISK_PCT_SHORT: float = Field(default=0.0030, ge=0.0, le=1.0)
    BASE_RISK_PCT_MEDIUM: float = Field(default=0.0025, ge=0.0, le=1.0)
    BASE_RISK_PCT_LONG: float = Field(default=0.0020, ge=0.0, le=1.0)
    TOTAL_MAX_HEAT_PCT: float = Field(default=0.0150, ge=0.0, le=1.0)
    SLEEVE_MAX_HEAT_SHORT: float = Field(default=0.0060, ge=0.0, le=1.0)
    SLEEVE_MAX_HEAT_MEDIUM: float = Field(default=0.0050, ge=0.0, le=1.0)
    SLEEVE_MAX_HEAT_LONG: float = Field(default=0.0040, ge=0.0, le=1.0)
    DAILY_LOSS_LIMIT_PCT: float = Field(default=0.020, ge=0.0, le=1.0)
    WEEKLY_LOSS_LIMIT_PCT: float = Field(default=0.050, ge=0.0, le=1.0)
    DAILY_MAX_LOSS_USDT: float = Field(default=10.0, ge=0.0)
    MAX_OPEN_POSITIONS: int = Field(default=1, ge=0, le=50)
    MAX_OPEN_POSITIONS_TOTAL: int = Field(default=3, ge=0, le=100)
    MAX_OPEN_POSITIONS_PER_SLEEVE: int = Field(default=1, ge=0, le=100)
    POSITION_DUPLICATE_BLOCK: bool = Field(default=True)
    COOLDOWN_SECONDS: int = Field(default=300, ge=0, le=86400)

    # Legacy/default paper sizing
    PAPER_USDT: float = Field(default=100.0, ge=0.0)
    TRAIL_PCT: float = Field(default=0.015, ge=0.0, le=0.2)
    DEFAULT_SL_PCT: float = Field(default=0.02, ge=0.0, le=0.2)
    DEFAULT_TP_PCT: float = Field(default=0.03, ge=0.0, le=0.5)

    # Regime / execution guards
    REGIME_GUARD_ENABLED: bool = Field(default=True)
    REGIME_MODEL: str = Field(default="rules_v1")
    REGIME_ALLOWED_SHORT: str = Field(default="BULLISH,NEUTRAL_UP")
    REGIME_ALLOWED_MEDIUM: str = Field(default="BULLISH,NEUTRAL_UP,RANGE")
    REGIME_ALLOWED_LONG: str = Field(default="BULLISH,NEUTRAL_UP")
    CORR_KILL_ENABLED: bool = Field(default=True)
    CORR_LOOKBACK_BARS: int = Field(default=96, ge=1)
    CORR_KILL_THRESHOLD: float = Field(default=0.85, ge=0.0, le=1.0)
    CORR_CLUSTER_MAX_COUNT: int = Field(default=2, ge=1)
    CORR_FREEZE_MINUTES: int = Field(default=30, ge=0)
    EXECUTION_HEALTH_SCALING_ENABLED: bool = Field(default=True)
    MAX_SPREAD_BPS: int = Field(default=12, ge=0)
    MAX_SLIPPAGE_BPS_SHORT: int = Field(default=10, ge=0)
    MAX_SLIPPAGE_BPS_MEDIUM: int = Field(default=14, ge=0)
    MAX_SLIPPAGE_BPS_LONG: int = Field(default=18, ge=0)
    API_ERROR_COOLDOWN_SECONDS: int = Field(default=180, ge=0)
    HTTP_429_COOLDOWN_SECONDS: int = Field(default=300, ge=0)

    # Audit / reporting
    SIGNAL_AUDIT_ENABLED: bool = Field(default=True)
    AUDIT_RECENT_WINDOW_HOURS: int = Field(default=24, ge=1)

    # Paper execution realism
    COMMISSION_RATE: float = Field(default=0.001, ge=0.0, le=0.01)
    PAPER_SLIPPAGE_BPS: float = Field(default=2.0, ge=0.0, le=100.0)
    PAPER_SLIPPAGE_JITTER_BPS: float = Field(default=1.0, ge=0.0, le=100.0)

    # Sleeve-level ATR params
    SHORT_STOP_ATR: float = Field(default=1.2, ge=0.0)
    SHORT_TP_ATR: float = Field(default=1.8, ge=0.0)
    SHORT_TRAIL_ATR: float = Field(default=0.8, ge=0.0)
    MEDIUM_STOP_ATR: float = Field(default=1.6, ge=0.0)
    MEDIUM_TP_ATR: float = Field(default=2.4, ge=0.0)
    MEDIUM_TRAIL_ATR: float = Field(default=1.0, ge=0.0)
    LONG_STOP_ATR: float = Field(default=2.0, ge=0.0)
    LONG_TP_ATR: float = Field(default=3.0, ge=0.0)
    LONG_TRAIL_ATR: float = Field(default=1.2, ge=0.0)

    # External API keys
    BINANCE_API_KEY: str = Field(default="")
    BINANCE_SECRET_KEY: str = Field(default="")
    CRYPTOPANIC_API_KEY: str = Field(default="")
    TELEGRAM_BOT_TOKEN: str = Field(default="")
    TELEGRAM_CHAT_ID: str = Field(default="")

    def __getattr__(self, name: str):
        upper_name = name.upper()
        model_fields = getattr(type(self), "model_fields", {})
        if upper_name != name and upper_name in model_fields:
            return object.__getattribute__(self, upper_name)
        raise AttributeError(f"{type(self).__name__!s} has no attribute {name!r}")

    @property
    def is_prod(self) -> bool:
        env_value = str(self.APP_ENV or self.ENV).lower()
        return env_value in {"prod", "production"}


settings = Settings()
DB_PATH = _resolve_runtime_path(settings.DB_PATH)
RESEARCH_DB_PATH = _resolve_runtime_path(settings.RESEARCH_DB_PATH)
