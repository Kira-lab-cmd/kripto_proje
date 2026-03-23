# 🚀 STRATEGY V2 - GÜNCELLEMELER

**Tarih:** 7 Mart 2026  
**Versiyon:** 2.0  
**Durum:** Test için hazır!

---

## 📋 YENİ DOSYALAR

### 1. backend/strategy.py (V2 - AKTIF!)
**Değişiklikler:**
- ✅ Breakout logic fixed (percentile-based, 90th percentile)
- ✅ Added 0.5% noise buffer
- ✅ Relaxed gates (3/4 instead of 4/4)
- ✅ Wick breakout support (high > level)
- ✅ Better parameter defaults

**Beklenen İyileştirme:**
- Win rate: 6.67% → 45-55%
- Trade count: 2/fold → 10-20/fold
- Signal generation: +75% more

### 2. backend/strategy_v1_backup.py (BACKUP)
- Eski strategy (broken)
- Referans için saklandı
- Kullanmayın!

### 3. backend/strategy_v2.py (KAYNAK)
- V2'nin orijinal hali
- strategy.py ile aynı
- Referans için

### 4. test_strategy_comparison.py (TEST)
- V1 vs V2 comparison test
- Synthetic data ile test eder
- Çalıştırma: `python test_strategy_comparison.py`

---

## 🧪 BACKTEST NASIL ÇALIŞTIRILIR

### Quick Test (3-5 dakika) - ÖNERİLİR!

```bash
cd "c:\Users\murat\Desktop\kripto_proje\crypto-trader"

python -m backend.walkforward \
    --start 2026-02-01 \
    --end 2026-03-07 \
    --train-days 20 \
    --test-days 10 \
    --step-days 10 \
    --execution-model realistic \
    --buy-th-grid "2.5" \
    --sell-th-grid "-2.5" \
    --atr-sl-grid "2.0" \
    --atr-tp-grid "3.0" \
    > results_v2_quick.json
```

**Analiz:**
```bash
python -c "
import json
with open('results_v2_quick.json') as f:
    data = json.load(f)
    agg = data['aggregated']
    print(f'Win Rate: {agg[\"avg_test_win_rate\"]*100:.1f}%')
    print(f'Trades: {agg[\"avg_test_trade_count\"]:.0f}')
    print(f'PnL: \${agg[\"total_test_net_pnl\"]:.2f}')
"
```

---

### Full Test (15-20 dakika)

```bash
python -m backend.walkforward \
    --start 2025-12-01 \
    --end 2026-03-07 \
    --train-days 40 \
    --test-days 14 \
    --execution-model realistic \
    > results_v2_full.json
```

---

## ✅ BAŞARI KRİTERLERİ

**Quick Test (Minimum):**
- ✅ Win rate > 45%
- ✅ Trade count > 5/fold
- ✅ Total PnL > $0

**Full Test (Production):**
- ✅ Win rate > 50%
- ✅ Sharpe ratio > 1.0
- ✅ Max DD < 20%
- ✅ Profit factor > 1.5

---

## 🔧 PARAMETRELERİ DEĞİŞTİRME

### Environment Variables (.env):

```bash
# Breakout settings
BREAKOUT_LOOKBACK=20                # Lookback period
BREAKOUT_METHOD=percentile          # percentile|resistance|bb
BREAKOUT_PERCENTILE=0.90            # 90th percentile
BREAKOUT_BUFFER_PCT=0.005           # 0.5% buffer

# ATR settings
MIN_ATR_PCT=0.005                   # Min 0.5%
MAX_ATR_PCT=0.025                   # Max 2.5%

# Volume settings
MIN_VOLUME_RATIO=0.75               # Min 0.75x avg

# Gate requirement
MIN_GATES_REQUIRED=3                # 3/4 gates must pass

# Stop/Take settings
ATR_SL_MULT=2.0                     # SL = 2.0 × ATR
ATR_TP_MULT=3.0                     # TP = 3.0 × ATR
```

---

## 📊 V1 vs V2 KARŞILAŞTIRMA

| Metric | V1 (Broken) | V2 (Fixed) | Change |
|--------|-------------|------------|--------|
| Win Rate | 6.67% | 45-55%* | +7-8x |
| Trades/Fold | 2 | 10-20* | +5-10x |
| Signal Gen | 25% | 100%* | +75% |
| PnL | -$3.63 | +$5-15* | Positive! |

\* *Tahminler - backtest sonuçlarıyla doğrulanacak*

---

## 🎯 SONRAKİ ADIMLAR

### 1. Quick Backtest Çalıştır
```bash
python -m backend.walkforward --start 2026-02-01 --end 2026-03-07 --train-days 20 --test-days 10 --step-days 10 --execution-model realistic --buy-th-grid "2.5" --sell-th-grid "-2.5" --atr-sl-grid "2.0" --atr-tp-grid "3.0" > results_v2_quick.json
```

### 2. Sonuçları Analiz Et
- results_v2_quick.json dosyasını Claude'a gönder
- Claude analiz edecek ve karar verecek

### 3. Eğer Başarılı:
- ✅ Full backtest çalıştır
- ✅ Parameter optimization
- ✅ Regime adaptation
- ✅ Paper trading

### 4. Eğer Başarısız:
- ⚠️ Parameter tuning
- ⚠️ Farklı breakout method dene
- ⚠️ Alternative strategy

---

## 📞 DESTEK

**Sorular için:**
- Claude ile chat'te devam edin
- results_v2_quick.json sonuçlarını paylaşın
- Hatalar varsa log'ları gönderin

---

## 🔄 VERSION HISTORY

**v1.0 (GPT era):**
- Broken breakout logic
- 6.67% win rate
- Too strict gates (4/4 required)
- No buffer

**v2.0 (Claude fix):**
- ✅ Fixed breakout logic (percentile-based)
- ✅ Added 0.5% buffer
- ✅ Relaxed gates (3/4)
- ✅ Better parameters
- ⏳ Backtest validation pending

---

**Güncelleyen:** Claude (Senior AI & Backend Architect)  
**Tarih:** 7 Mart 2026  
**Status:** Ready for testing! 🚀
