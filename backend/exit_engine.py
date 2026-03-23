# File: backend/exit_engine.py
"""
Exit Engine v0.1

Grid pozisyonları normalde crossing mantığıyla kapatılır.
Bu modül ek çıkış koşulları ekler:

1. Yaş bazlı çıkış (AGE_EXIT):
   - GRID_MAX_POSITION_AGE_H saatten eski pozisyon → kapat
   - Varsayılan: 48 saat

2. Fee-adjusted minimum kâr (FEE_EXIT):
   - Gross PnL komisyonu karşılamıyorsa bile minimum süre geçmişse kapat
   - GRID_FEE_EXIT_MIN_PROFIT_PCT: brüt PnL % eşiği (default 0.001)
   - GRID_FEE_EXIT_MIN_AGE_H: bu kural için minimum pozisyon yaşı (default 4 saat)

3. Stop loss (STOP_EXIT):
   - GRID_EXIT_STOP_PCT: entry'den bu kadar düşünce kapat (default %3)
   - Watchdog'daki emergency exit'ten farklı: daha hassas, sadece bireysel pos.

Her çalışmada (watchdog tick'inde çağrılır) açık pozisyonları tarar,
koşullar sağlanmışsa trader üzerinden SELL emri verir.
"""

from __future__ import annotations

import logging
import os
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .trader import Trader
    from .database import Database

logger = logging.getLogger(__name__)


def _env_float(key: str, default: float) -> float:
    try:
        v = os.getenv(key, "").strip()
        return float(v) if v else default
    except (ValueError, TypeError):
        return default


def _env_bool(key: str, default: bool = False) -> bool:
    v = (os.getenv(key, "").strip() or "").lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return default


class ExitEngine:
    """
    Pozisyon çıkış motoru.

    Kullanım (watchdog loop içinden):
        exit_engine.run(positions, current_prices)
    """

    def __init__(self, db: "Database", trader: "Trader") -> None:
        self.db = db
        self.trader = trader
        self._last_run_ts: float = 0.0
        # En az 30 saniyede bir çalış (watchdog her 5s çalışır)
        self.min_interval_s: float = 30.0

    # ------------------------------------------------------------------
    # Ana çalıştırma
    # ------------------------------------------------------------------

    def run(self, positions: list[dict], price_cache: dict[str, float]) -> int:
        """
        Açık pozisyonları incele, çıkış koşulları sağlanmışsa kapat.
        Returns: kapanan pozisyon sayısı
        """
        now = time.time()
        if now - self._last_run_ts < self.min_interval_s:
            return 0
        self._last_run_ts = now

        if not _env_bool("GRID_EXIT_ENGINE_ENABLED", default=True):
            return 0

        logger.info(
            "exit_engine_run positions=%d cache_symbols=%s",
            len(positions), list(price_cache.keys())
        )
        closed = 0
        for pos in positions:
            try:
                result = self._evaluate_position(pos, price_cache)
                if result:
                    closed += 1
            except Exception as e:
                logger.warning("exit_engine position eval error sym=%s err=%s",
                               pos.get("symbol"), e)

        return closed

    # ------------------------------------------------------------------
    # Pozisyon değerlendirme
    # ------------------------------------------------------------------

    def _evaluate_position(self, pos: dict, price_cache: dict[str, float]) -> bool:
        """
        Tek pozisyonu değerlendir. True döner = kapatıldı.
        """
        strategy = str(pos.get("strategy_name") or "").lower().strip()
        if strategy != "grid_v1":
            return False  # Sadece grid pozisyonları

        sym = str(pos.get("symbol") or "")
        if not sym:
            return False

        px = price_cache.get(sym)
        if not px or px <= 0:
            return False

        entry = float(pos.get("entry_price") or 0)
        if entry <= 0:
            return False

        # Pozisyon yaşı (saniye)
        created_at = pos.get("opened_at") or pos.get("created_at") or pos.get("updated_at")
        age_h = self._age_hours(created_at)

        pnl_pct = (px - entry) / entry  # gross, fee'siz

        # Grid inventory pseudo-pozisyonları: sadece AGE exit uygula
        # Stop-loss, trailing stop, RSI exit bu pozisyonlara uygulanmaz
        # (bunlar positions tablosunda değil, grid_state'te yaşıyor)
        is_grid_inventory = bool(pos.get("grid_inventory"))

        # ── 1. Stop loss ────────────────────────────────────────────────
        stop_pct = _env_float("GRID_EXIT_STOP_PCT", 0.03)
        if not is_grid_inventory and pnl_pct <= -stop_pct:
            return self._close(pos, px, f"EXIT_STOP_LOSS({pnl_pct*100:.2f}%)")

        # ── 2. Trailing Stop ─────────────────────────────────────────────
        # Fiyat zirve yaptıktan sonra geri dönünce erken çık
        trail_pct = _env_float("GRID_TRAIL_STOP_PCT", 0.015)  # default %1.5
        if not is_grid_inventory and trail_pct > 0:
            # DB'deki en yüksek fiyatı güncelle
            high_px = pos.get("highest_price") or entry
            if px > high_px:
                high_px = px
                try:
                    self.db.update_highest_price(sym, px)
                except Exception:
                    pass
            # Zirvenin trail_pct altına düştüyse ve kârın en az yarısı gidiyorsa çık
            trail_trigger = high_px * (1 - trail_pct)
            peak_gain = (high_px - entry) / entry if entry > 0 else 0
            if px <= trail_trigger and peak_gain > trail_pct:
                # Zirveden belirli kadar gerildi VE gerçekten yükselmişti
                return self._close(
                    pos, px,
                    f"EXIT_TRAIL(peak={high_px:.2f} trail={trail_trigger:.2f} pnl={pnl_pct*100:.2f}%)"
                )

        # ── 3. RSI Aşırı Alım Sonrası Düşüş ─────────────────────────────
        # Mantık: RSI önce eşiği AŞMALI, sonra geri düşünce çık
        # Böylece zaten aşırı alım yaşanmadan tetiklenmez
        rsi_exit_enabled = _env_bool("GRID_RSI_EXIT_ENABLED", default=True)
        if not is_grid_inventory and rsi_exit_enabled:
            try:
                ohlcv = self._get_ohlcv(sym)
                if ohlcv and len(ohlcv) >= 15:
                    rsi = self._calc_rsi(ohlcv, period=14)
                    er = self._calc_er(ohlcv, period=10)
                    peak_gain = ((pos.get("highest_price") or entry) - entry) / entry if entry > 0 else 0
                    rsi_threshold = _env_float("GRID_RSI_EXIT_THRESHOLD", 70.0)
                    er_threshold = _env_float("GRID_RSI_EXIT_ER_MAX", 0.3)
                    # RSI'ın geçmişte eşiği aştığını kontrol et (son 5 mum)
                    recent_rsi = [self._calc_rsi(ohlcv[:-i] if i > 0 else ohlcv, period=14) for i in range(0, 5)]
                    rsi_was_overbought = any(r >= rsi_threshold for r in recent_rsi)
                    # Koşullar: RSI önce 70+ iken zirve, şimdi 70 altına düştü + ER düşük + kâr var
                    if rsi_was_overbought and rsi < rsi_threshold and er < er_threshold and peak_gain > 0.005:
                        logger.info(
                            "exit_engine_rsi_exit sym=%s rsi=%.1f er=%.3f peak_gain=%.2f%% px=%.4f",
                            sym, rsi, er, peak_gain * 100, px
                        )
                        return self._close(
                            pos, px,
                            f"EXIT_RSI_DECAY(rsi={rsi:.1f} er={er:.3f} peak={peak_gain*100:.2f}%)"
                        )
            except Exception as _rsi_err:
                logger.debug("exit_engine rsi_check error: %s", _rsi_err)

        # ── 4. Yaş bazlı çıkış (kademeli zarar toleransı) ──────────────
        # 48s+: -%2'ye kadar tolere et → kapat
        # 72s+: -%4'e kadar tolere et → kapat
        # 96s+: zarar ne olursa olsun kapat (sermayeyi serbest bırak)
        max_age_h = _env_float("GRID_MAX_POSITION_AGE_H", 48.0)
        age_loss_tolerance_48h = _env_float("GRID_AGE_EXIT_LOSS_48H", 0.02)   # -%2
        age_loss_tolerance_72h = _env_float("GRID_AGE_EXIT_LOSS_72H", 0.04)   # -%4
        age_force_close_h = _env_float("GRID_AGE_EXIT_FORCE_H", 96.0)         # zorla kapat

        if age_h >= age_force_close_h:
            logger.warning(
                "exit_engine_age_force sym=%s age=%.1fh pnl=%.2f%% → zorla kapanıyor",
                sym, age_h, pnl_pct * 100
            )
            return self._close(pos, px, f"EXIT_AGE_FORCE({age_h:.1f}h pnl={pnl_pct*100:.2f}%)")

        elif age_h >= 72.0 and pnl_pct >= -age_loss_tolerance_72h:
            logger.warning(
                "exit_engine_age_72h sym=%s age=%.1fh pnl=%.2f%% → 72s zarar toleransı",
                sym, age_h, pnl_pct * 100
            )
            return self._close(pos, px, f"EXIT_AGE_72H({age_h:.1f}h pnl={pnl_pct*100:.2f}%)")

        elif age_h >= max_age_h and pnl_pct >= -age_loss_tolerance_48h:
            logger.warning(
                "exit_engine_age_48h sym=%s age=%.1fh pnl=%.2f%% → 48s zarar toleransı",
                sym, age_h, pnl_pct * 100
            )
            return self._close(pos, px, f"EXIT_AGE_48H({age_h:.1f}h pnl={pnl_pct*100:.2f}%)")

        # ── 5. Fee-adjusted minimum kâr çıkışı ──────────────────────────
        fee_min_pct = _env_float("GRID_FEE_EXIT_MIN_PROFIT_PCT", 0.001)
        fee_min_age_h = _env_float("GRID_FEE_EXIT_MIN_AGE_H", 4.0)
        if age_h >= fee_min_age_h and pnl_pct >= fee_min_pct:
            # Yeterince bekledik ve en azından fee'yi karşılayan kâr var
            # NOT: crossing bunu zaten yakalamalı; bu fallback
            logger.debug(
                "exit_engine fee_exit candidate sym=%s age=%.1fh pnl=%.3f%%",
                sym, age_h, pnl_pct * 100,
            )
            # Bu kural tek başına kapatmaz — crossing beklemeye devam et
            # Sadece loglama amaçlı. İleride aktif hale getirilebilir:
            # return self._close(pos, px, f"EXIT_FEE_PROFIT({pnl_pct*100:.2f}%)")

        return False

    # ------------------------------------------------------------------
    # Yardımcılar
    # ------------------------------------------------------------------

    def _get_ohlcv(self, symbol: str) -> list | None:
        """OHLCV verisini binance_service üzerinden al."""
        try:
            from .binance_service import BinanceService
            import os
            # Global binance_svc referansını al
            import backend.main as _main
            svc = getattr(_main, "binance_svc", None)
            if svc:
                return svc.get_historical_data(symbol, "15m", limit=20)
        except Exception:
            pass
        return None

    def _calc_rsi(self, ohlcv: list, period: int = 14) -> float:
        """Basit RSI hesapla."""
        closes = [float(c[4]) for c in ohlcv[-period-1:]]
        if len(closes) < period + 1:
            return 50.0
        gains, losses = [], []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i-1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _calc_er(self, ohlcv: list, period: int = 10) -> float:
        """Efficiency Ratio hesapla."""
        closes = [float(c[4]) for c in ohlcv[-period-1:]]
        if len(closes) < period + 1:
            return 0.5
        direction = abs(closes[-1] - closes[0])
        volatility = sum(abs(closes[i] - closes[i-1]) for i in range(1, len(closes)))
        return direction / volatility if volatility > 0 else 0.0

    def _age_hours(self, created_at) -> float:
        """Pozisyon yaşını saat cinsinden döndür."""
        if created_at is None:
            return 0.0
        try:
            import datetime
            if isinstance(created_at, (int, float)):
                ts = float(created_at)
            elif isinstance(created_at, str):
                # ISO format veya timestamp string
                try:
                    ts = float(created_at)
                except ValueError:
                    dt = datetime.datetime.fromisoformat(
                        created_at.replace("Z", "+00:00")
                    )
                    ts = dt.timestamp()
            else:
                return 0.0
            age_s = time.time() - ts
            return max(0.0, age_s / 3600.0)
        except Exception:
            return 0.0

    def _close(self, pos: dict, px: float, reason: str) -> bool:
        """Pozisyonu kapat. Tüm exit_engine kapanışları emergency=True ile çalışır
        — min_cost/min_notional guard'ları bypass edilir, tüm bakiye satılır."""
        sym = str(pos.get("symbol") or "")
        entry = float(pos.get("entry_price") or 0)
        pnl_pct = (px - entry) / entry if entry > 0 else 0.0

        logger.warning(
            "exit_engine_close sym=%s px=%.4f entry=%.4f pnl=%.2f%% reason=%s",
            sym, px, entry, pnl_pct * 100, reason,
        )
        try:
            # signal_details üzerinden emergency flag gönder
            self.trader.execute_trade(
                sym, "SELL", px, 1.0,
                reason=reason,
                signal_details={"_is_emergency": True, "exit_engine": True},
            )
            return True
        except Exception as e:
            logger.error("exit_engine execute_trade failed sym=%s err=%s", sym, e)
            return False