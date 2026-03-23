#!/usr/bin/env python3
"""
Grid Strategy Backtester & Parameter Optimizer
===============================================
Research.db'deki OHLCV verisiyle grid parametrelerini backtest eder
ve en iyi kombinasyonu bulur.

Kullanım:
  # Tek backtest
  python -m backend.grid_optimizer --symbol BTC/USDT --width 0.08 --count 40

  # Parametre optimizasyonu (tüm kombinasyonlar)
  python -m backend.grid_optimizer --optimize --symbol BTC/USDT

  # Her iki sembol optimize et
  python -m backend.grid_optimizer --optimize --symbol BTC/USDT ETH/USDT
"""
from __future__ import annotations

import argparse
import itertools
import json
import os
import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ── Sabitler ─────────────────────────────────────────────────────────────────
COMMISSION = 0.001          # %0.1 Binance Spot
SLIPPAGE_BPS = 2.0          # ortalama kayma bips
BACKEND_DIR = Path(__file__).resolve().parent
DEFAULT_DB = BACKEND_DIR / "research.db"

UTC = timezone.utc

# ── Veri yükleme ─────────────────────────────────────────────────────────────

def load_ohlcv(symbol: str, timeframe: str = "15m", db_path: Path = DEFAULT_DB) -> list[dict]:
    """Research DB'den OHLCV satırlarını yükle, ts_ms'e göre sırala."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT ts_ms, open, high, low, close, volume FROM ohlcv "
        "WHERE symbol=? AND timeframe=? ORDER BY ts_ms ASC",
        (symbol, timeframe),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    if not rows:
        raise ValueError(f"Veri bulunamadı: {symbol} {timeframe} — önce /research/ingest çağır")
    return rows


# ── Grid motoru ──────────────────────────────────────────────────────────────

@dataclass
class GridParams:
    symbol: str
    grid_width_pct: float       # ±% aralık (0.08 = ±8%)
    grid_count: int             # seviye sayısı
    capital_per_grid: float     # her seviyeye USDT
    sell_only_profitable: bool = True
    min_profit_pct: float = 0.001
    emergency_stop_pct: float = 0.05   # grid aralığının dışına bu kadar çıkınca kapat
    block_buy_in_trend: bool = True
    trend_adx_min: float = 22.0
    trend_er_min: float = 0.40


@dataclass
class GridBacktestResult:
    symbol: str
    params: GridParams
    start_date: str
    end_date: str
    initial_capital: float
    final_capital: float
    net_pnl: float
    net_pnl_pct: float
    total_trades: int
    buy_trades: int
    sell_trades: int
    wins: int
    losses: int
    win_rate: float
    avg_win_pct: float
    avg_loss_pct: float
    profit_factor: float
    max_drawdown_pct: float
    total_fees: float
    grid_rounds: int        # tamamlanan BUY+SELL çiftleri
    trend_blocks: int       # TREND nedeniyle blok'lanan BUY sayısı
    emergency_exits: int
    sharpe_approx: float    # yaklaşık Sharpe (günlük PnL std bazlı)
    score: float            # bileşik skor (optimizasyonda sıralama için)


def _calc_adx_er(closes: list[float], highs: list[float], lows: list[float],
                 adx_period: int = 14, er_period: int = 20) -> tuple[float | None, float | None]:
    """Basit ADX ve Kaufman ER hesabı."""
    n = len(closes)
    if n < adx_period * 3:
        return None, None

    # Kaufman ER
    er = None
    if n >= er_period + 1:
        change = abs(closes[-1] - closes[-(er_period + 1)])
        diffs = sum(abs(closes[i] - closes[i - 1]) for i in range(n - er_period, n))
        er = change / diffs if diffs > 1e-12 else 0.0
        er = max(0.0, min(1.0, er))

    # Basit ADX
    adx = None
    try:
        ups, downs, trs = [], [], []
        for i in range(1, min(adx_period * 3, n)):
            up = highs[-i] - highs[-(i + 1)]
            dn = lows[-(i + 1)] - lows[-i]
            plus_dm = up if up > dn and up > 0 else 0.0
            minus_dm = dn if dn > up and dn > 0 else 0.0
            tr = max(highs[-i] - lows[-i],
                     abs(highs[-i] - closes[-(i + 1)]),
                     abs(lows[-i]  - closes[-(i + 1)]))
            ups.append(plus_dm)
            downs.append(minus_dm)
            trs.append(tr)

        if len(trs) >= adx_period:
            def rma(vals, p):
                res = sum(vals[:p]) / p
                for v in vals[p:]:
                    res = (res * (p - 1) + v) / p
                return res

            atr_v = rma(trs, adx_period) or 1e-12
            plus_di  = 100 * rma(ups,   adx_period) / atr_v
            minus_di = 100 * rma(downs, adx_period) / atr_v
            dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-12)
            adx = dx  # tek periyot DX'i ADX yaklaşımı olarak kullan
    except Exception:
        adx = None

    return adx, er


def run_grid_backtest(
    ohlcv: list[dict],
    params: GridParams,
    initial_capital: float = 1000.0,
    verbose: bool = False,
) -> GridBacktestResult:
    """
    Tek parametre seti için grid backtest çalıştır.

    Simülasyon mantığı:
    - Fiyat level[i]'yi AŞAĞI geçerse → level[i]'de BUY aç
    - Fiyat level[i+1]'i YUKARI geçerse → level[i]'deki pozisyonu kapat (SELL)
    - Bir alt seviyede BUY varken üst seviyeye çıkınca SELL tetiklenir
    - TREND rejiminde yeni BUY açılmaz (block_buy_in_trend=True ise)
    - Grid aralığı dışına çıkılırsa tüm pozisyonlar kapatılır
    """
    if len(ohlcv) < 200:
        raise ValueError("Yetersiz veri (min 200 mum)")

    closes = [float(r["close"]) for r in ohlcv]
    highs  = [float(r["high"])  for r in ohlcv]
    lows   = [float(r["low"])   for r in ohlcv]

    # Başlangıç grid'ini kur
    init_price = closes[200]
    w = params.grid_width_pct
    grid_lower = init_price * (1 - w)
    grid_upper = init_price * (1 + w)
    step = (grid_upper - grid_lower) / params.grid_count
    grid_levels = [grid_lower + i * step for i in range(params.grid_count + 1)]

    # Grid envanter: her seviyede ne kadar coin var
    grid_inventory: dict[float, float] = {g: 0.0 for g in grid_levels}
    grid_entry_price: dict[float, float] = {}   # bu seviyedeki avg giriş fiyatı

    capital = initial_capital
    total_fees = 0.0
    buys = sells = wins = losses = 0
    trend_blocks = emergency_exits = grid_rounds = 0
    pnl_log: list[float] = []           # her trade PnL'i (drawdown için)
    last_price = closes[200]

    peak_capital = capital
    max_drawdown = 0.0
    daily_pnl: list[float] = []         # Sharpe için

    slip_factor = 1.0 + (SLIPPAGE_BPS / 10000)

    for i in range(201, len(ohlcv)):
        bar = ohlcv[i]
        bar_open  = float(bar["open"])
        bar_high  = float(bar["high"])
        bar_low   = float(bar["low"])
        bar_close = float(bar["close"])

        # Trend tespiti (her 20 barda bir, hızlı)
        is_trend = False
        if params.block_buy_in_trend and i % 20 == 0:
            adx, er = _calc_adx_er(closes[:i], highs[:i], lows[:i])
            if adx is not None and er is not None:
                is_trend = (adx >= params.trend_adx_min and er >= params.trend_er_min)

        # Emergency exit: grid aralığının dışına çıktık mı?
        range_size = grid_upper - grid_lower
        emergency_band = range_size * params.emergency_stop_pct
        # Emergency: bar'ın KAPANIS fiyatı aralık dışındaysa tetikle (bar_low çok geniş)
        if bar_close < (grid_lower - emergency_band) or bar_close > (grid_upper + emergency_band):
            # Tüm envanteri sat
            for gp, qty in list(grid_inventory.items()):
                if qty > 0:
                    exec_price = bar_open * (1 - SLIPPAGE_BPS / 10000)
                    revenue = qty * exec_price * (1 - COMMISSION)
                    entry = grid_entry_price.get(gp, exec_price)
                    pnl = revenue - (qty * entry)
                    capital += revenue
                    total_fees += qty * exec_price * COMMISSION
                    if pnl > 0:
                        wins += 1
                    else:
                        losses += 1
                    sells += 1
                    emergency_exits += 1
                    pnl_log.append(pnl)
                    grid_inventory[gp] = 0.0
            # Grid'i yeniden kur
            init_price2 = bar_close
            grid_lower = init_price2 * (1 - w)
            grid_upper = init_price2 * (1 + w)
            step = (grid_upper - grid_lower) / params.grid_count
            grid_levels = [grid_lower + i2 * step for i2 in range(params.grid_count + 1)]
            grid_inventory = {g: 0.0 for g in grid_levels}
            grid_entry_price = {}
            last_price = bar_close
            continue  # yeniden kurulduktan sonra bu barda işlem yapma

        # Grid crossing mantığı: BUY@level[i], SELL@level[i+1]
        # ─────────────────────────────────────────────────────
        # SELL taraması: Her açık BUY pozisyonunun bir ÜST seviyesi geçildi mi?
        for idx, gp_buy in enumerate(grid_levels[:-1]):
            qty = grid_inventory.get(gp_buy, 0.0)
            if qty <= 0:
                continue
            gp_sell = grid_levels[idx + 1]          # bir üst seviye = hedef satış fiyatı
            if last_price < gp_sell <= bar_high:     # fiyat üst seviyeyi yukarı geçti
                exec_price = gp_sell * (1.0 - SLIPPAGE_BPS / 10000.0)
                entry_cost_total = qty * grid_entry_price.get(gp_buy, gp_buy * slip_factor)
                revenue = qty * exec_price * (1.0 - COMMISSION)
                pnl = revenue - entry_cost_total
                if params.sell_only_profitable and pnl / max(entry_cost_total, 1e-9) < params.min_profit_pct:
                    continue
                capital += revenue
                total_fees += qty * exec_price * COMMISSION
                if pnl > 0:
                    wins += 1
                    grid_rounds += 1
                else:
                    losses += 1
                sells += 1
                pnl_log.append(pnl)
                grid_inventory[gp_buy] = 0.0
                if gp_buy in grid_entry_price:
                    del grid_entry_price[gp_buy]

        # BUY taraması: Fiyat hangi seviyenin ALTINA düştü?
        buy_candidates = []
        for idx, gp in enumerate(grid_levels[:-1]):
            if last_price > gp >= bar_low and grid_inventory.get(gp, 0.0) == 0.0:
                buy_candidates.append(gp)
        if buy_candidates:
            gp = max(buy_candidates)          # fiyata en yakın (en yüksek) BUY seviyesi
            if is_trend:
                trend_blocks += 1
            else:
                qty_to_buy = params.capital_per_grid / gp
                buy_exec_price = gp * slip_factor
                cost = qty_to_buy * buy_exec_price * (1.0 + COMMISSION)
                if cost <= capital:
                    capital -= cost
                    total_fees += qty_to_buy * buy_exec_price * COMMISSION
                    grid_inventory[gp] = qty_to_buy
                    grid_entry_price[gp] = cost / qty_to_buy   # gerçek birim maliyet
                    buys += 1

        # Açık pozisyonları mark-to-market et (drawdown hesabı için)
        open_value = sum(qty * bar_close for qty in grid_inventory.values() if qty > 0)
        equity = capital + open_value
        if equity > peak_capital:
            peak_capital = equity
        dd = (peak_capital - equity) / peak_capital * 100 if peak_capital > 0 else 0.0
        if dd > max_drawdown:
            max_drawdown = dd

        # Günlük PnL (Sharpe için ~96 bar = 1 gün 15m'de)
        if i % 96 == 0:
            daily_pnl.append(equity - initial_capital)

        last_price = bar_close

    # Son pozisyonları kapat
    final_price = closes[-1]
    open_value = sum(qty * final_price for qty in grid_inventory.values() if qty > 0)
    final_capital = capital + open_value

    total_trades = buys + sells
    win_rate = wins / sells if sells > 0 else 0.0
    net_pnl = final_capital - initial_capital
    net_pnl_pct = net_pnl / initial_capital * 100

    avg_win = sum(p for p in pnl_log if p > 0) / max(1, sum(1 for p in pnl_log if p > 0))
    avg_loss = sum(p for p in pnl_log if p < 0) / max(1, sum(1 for p in pnl_log if p < 0))
    gross_profit = sum(p for p in pnl_log if p > 0)
    gross_loss   = abs(sum(p for p in pnl_log if p < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)

    avg_win_pct  = avg_win  / initial_capital * 100
    avg_loss_pct = avg_loss / initial_capital * 100

    # Yaklaşık Sharpe
    if len(daily_pnl) >= 5:
        import statistics
        diffs = [daily_pnl[i] - daily_pnl[i-1] for i in range(1, len(daily_pnl))]
        std = statistics.stdev(diffs) if len(diffs) > 1 else 1e-9
        mean = statistics.mean(diffs)
        sharpe_approx = (mean / std) * (252 ** 0.5) if std > 0 else 0.0
    else:
        sharpe_approx = 0.0

    # Bileşik skor: kâr + win_rate + işlem sayısı (az işlemle aynı kâr = daha iyi)
    trade_penalty = max(0, (total_trades - 200)) * 0.001  # çok fazla işlem ceza alır
    score = (net_pnl_pct * 0.5) + (win_rate * 30) + (min(profit_factor, 5.0) * 5) - max_drawdown * 0.3 - trade_penalty

    start_dt = datetime.fromtimestamp(ohlcv[0]["ts_ms"] / 1000, tz=UTC).strftime("%Y-%m-%d")
    end_dt   = datetime.fromtimestamp(ohlcv[-1]["ts_ms"] / 1000, tz=UTC).strftime("%Y-%m-%d")

    return GridBacktestResult(
        symbol=params.symbol,
        params=params,
        start_date=start_dt,
        end_date=end_dt,
        initial_capital=initial_capital,
        final_capital=round(final_capital, 2),
        net_pnl=round(net_pnl, 2),
        net_pnl_pct=round(net_pnl_pct, 3),
        total_trades=total_trades,
        buy_trades=buys,
        sell_trades=sells,
        wins=wins,
        losses=losses,
        win_rate=round(win_rate, 4),
        avg_win_pct=round(avg_win_pct, 4),
        avg_loss_pct=round(avg_loss_pct, 4),
        profit_factor=round(min(profit_factor, 99.0), 3),
        max_drawdown_pct=round(max_drawdown, 2),
        total_fees=round(total_fees, 4),
        grid_rounds=grid_rounds,
        trend_blocks=trend_blocks,
        emergency_exits=emergency_exits,
        sharpe_approx=round(sharpe_approx, 3),
        score=round(score, 3),
    )


# ── Optimizasyon ─────────────────────────────────────────────────────────────

OPTIMIZE_GRID = {
    "grid_width_pct":    [0.06, 0.08, 0.10, 0.12],
    "grid_count":        [30, 40, 50, 60],
    "capital_per_grid":  [10.0, 15.0, 20.0],
    "sell_only_profitable": [True],
    "block_buy_in_trend":   [True, False],
}


def optimize(
    symbol: str,
    ohlcv: list[dict],
    initial_capital: float = 1000.0,
    top_n: int = 5,
    param_grid: dict | None = None,
) -> list[GridBacktestResult]:
    grid = param_grid or OPTIMIZE_GRID
    keys = list(grid.keys())
    combos = list(itertools.product(*[grid[k] for k in keys]))
    total = len(combos)

    print(f"\n{'='*60}")
    print(f"  {symbol} — {total} kombinasyon test ediliyor")
    print(f"  Veri: {len(ohlcv)} bar  |  Sermaye: ${initial_capital:,.0f}")
    print(f"{'='*60}\n")

    results: list[GridBacktestResult] = []
    for i, combo in enumerate(combos, 1):
        kw = dict(zip(keys, combo))
        p = GridParams(symbol=symbol, **kw)
        try:
            r = run_grid_backtest(ohlcv, p, initial_capital=initial_capital)
            results.append(r)
            if i % 20 == 0 or i == total:
                best_so_far = max(results, key=lambda x: x.score)
                print(f"  [{i:4}/{total}] En iyi şu an: score={best_so_far.score:.1f} "
                      f"pnl={best_so_far.net_pnl_pct:.2f}% "
                      f"wr={best_so_far.win_rate*100:.1f}% "
                      f"width={best_so_far.params.grid_width_pct} "
                      f"count={best_so_far.params.grid_count}")
        except Exception as e:
            print(f"  [{i:4}/{total}] HATA: {e}")

    results.sort(key=lambda x: x.score, reverse=True)
    return results[:top_n]


# ── Raporlama ────────────────────────────────────────────────────────────────

def print_result(r: GridBacktestResult, rank: int = 1) -> None:
    p = r.params
    verdict = "✅ KAR" if r.net_pnl > 0 else "❌ ZARAR"
    print(f"""
{'─'*55}
  #{rank}  {r.symbol}  {verdict}
{'─'*55}
  Parametreler:
    grid_width_pct    = {p.grid_width_pct}   (±{p.grid_width_pct*100:.0f}%)
    grid_count        = {p.grid_count}
    capital_per_grid  = ${p.capital_per_grid:.0f}
    block_buy_in_trend= {p.block_buy_in_trend}
    sell_only_profit  = {p.sell_only_profitable}

  Sonuçlar  ({r.start_date} → {r.end_date}):
    Net PnL           = ${r.net_pnl:+.2f}  ({r.net_pnl_pct:+.2f}%)
    Win Rate          = {r.win_rate*100:.1f}%  ({r.wins}W / {r.losses}L)
    Profit Factor     = {r.profit_factor:.2f}
    Max Drawdown      = {r.max_drawdown_pct:.2f}%
    Sharpe (approx)   = {r.sharpe_approx:.2f}
    Grid rounds       = {r.grid_rounds}
    Total trades      = {r.total_trades}  (buy={r.buy_trades} sell={r.sell_trades})
    Trend blocks      = {r.trend_blocks}
    Emergency exits   = {r.emergency_exits}
    Total fees        = ${r.total_fees:.2f}
    Score             = {r.score:.2f}
""")


def result_to_env(r: GridBacktestResult) -> str:
    """En iyi sonucu .env snippet'ine çevir."""
    p = r.params
    base = p.symbol.split("/")[0]
    lines = [
        f"# {p.symbol} — backtest sonucu (score={r.score:.1f}, pnl={r.net_pnl_pct:+.2f}%)",
        f"{base}_GRID_WIDTH_PCT={p.grid_width_pct}",
        f"GRID_COUNT={p.grid_count}",
        f"GRID_CAPITAL_PER_LEVEL={p.capital_per_grid}",
        f"GRID_BLOCK_BUY_IN_TREND={'true' if p.block_buy_in_trend else 'false'}",
        f"GRID_SELL_ONLY_PROFITABLE={'true' if p.sell_only_profitable else 'false'}",
    ]
    return "\n".join(lines)


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Grid Strategy Backtest & Optimizer")
    parser.add_argument("--symbol",    nargs="+", default=["BTC/USDT"], help="Sembol(ler)")
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--capital",   type=float, default=1000.0)
    parser.add_argument("--optimize",  action="store_true", help="Parametre optimizasyonu çalıştır")
    parser.add_argument("--top",       type=int, default=5, help="En iyi N sonucu göster")
    parser.add_argument("--out",       default="grid_optimize_results.json", help="JSON çıktı dosyası")
    parser.add_argument("--db",        default=str(DEFAULT_DB), help="Research DB yolu")

    # Tek backtest parametreleri
    parser.add_argument("--width",   type=float, default=0.08)
    parser.add_argument("--count",   type=int,   default=40)
    parser.add_argument("--cap",     type=float, default=10.0, help="capital_per_grid")
    parser.add_argument("--no-trend-block", action="store_true")

    args = parser.parse_args()
    db_path = Path(args.db)

    all_best: list[GridBacktestResult] = []

    for symbol in args.symbol:
        try:
            ohlcv = load_ohlcv(symbol, args.timeframe, db_path)
            print(f"\n  {symbol}: {len(ohlcv)} bar yüklendi "
                  f"({datetime.fromtimestamp(ohlcv[0]['ts_ms']/1000, tz=UTC).strftime('%Y-%m-%d')} → "
                  f"{datetime.fromtimestamp(ohlcv[-1]['ts_ms']/1000, tz=UTC).strftime('%Y-%m-%d')})")
        except ValueError as e:
            print(f"  HATA: {e}")
            continue

        if args.optimize:
            top_results = optimize(symbol, ohlcv, initial_capital=args.capital, top_n=args.top)
            print(f"\n  ══ {symbol} TOP {args.top} SONUÇ ══")
            for rank, r in enumerate(top_results, 1):
                print_result(r, rank)
            if top_results:
                all_best.append(top_results[0])
        else:
            # Tek backtest
            p = GridParams(
                symbol=symbol,
                grid_width_pct=args.width,
                grid_count=args.count,
                capital_per_grid=args.cap,
                block_buy_in_trend=not args.no_trend_block,
            )
            r = run_grid_backtest(ohlcv, p, initial_capital=args.capital, verbose=True)
            print_result(r)
            all_best.append(r)

    if not all_best:
        print("\n❌ Hiç sonuç üretilemedi.")
        return

    # .env önerileri
    print("\n" + "="*60)
    print("  📝 .env İÇİN ÖNERİLEN PARAMETRELER")
    print("="*60)
    for r in all_best:
        print(f"\n{result_to_env(r)}")

    # JSON kaydet
    out_path = Path(args.out)
    output = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "best_per_symbol": [
            {
                "symbol": r.symbol,
                "score": r.score,
                "net_pnl_pct": r.net_pnl_pct,
                "win_rate": r.win_rate,
                "max_drawdown_pct": r.max_drawdown_pct,
                "params": asdict(r.params),
                "env_snippet": result_to_env(r),
            }
            for r in all_best
        ],
    }
    out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"\n  💾 Sonuçlar kaydedildi: {out_path}")
    print()


if __name__ == "__main__":
    main()
