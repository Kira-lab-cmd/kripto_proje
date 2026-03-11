@echo off
echo ================================================================================
echo GRID TRADING - FINAL BACKTEST (ALL BUGS FIXED!)
echo ================================================================================
echo.
echo Bu script:
echo   1. Cache temizler
echo   2. Backtest calistirir
echo   3. Sonuclari gosterir
echo.
pause

echo.
echo [1/3] Cache temizleniyor...
powershell -Command "Get-ChildItem -Recurse -Directory -Filter '__pycache__' | Remove-Item -Recurse -Force"
powershell -Command "Get-ChildItem -Recurse -Filter '*.pyc' | Remove-Item -Force"
echo ✅ Cache temizlendi!

echo.
echo [2/3] Backtest calistiriliyor...
echo Grid Range: $75,000 - $105,000
echo Period: December 2025
echo Expected: 100-200 trades
echo.

python -m backend.walkforward --start 2025-12-01 --end 2025-12-31 --train-days 7 --test-days 7 > results_REAL_FINAL.json

if errorlevel 1 (
    echo.
    echo ❌ Backtest FAILED!
    pause
    exit /b 1
)

echo ✅ Backtest tamamlandi!
echo.

echo [3/3] Sonuclar analiz ediliyor...
echo.

python -c "import json; data=json.load(open('results_REAL_FINAL.json', 'r', encoding='utf-16-le')); agg=data.get('aggregated', {}); trades=agg.get('total_trades', 0); print('='*80); print('📊 BACKTEST SONUÇLARI'); print('='*80); print(); print(f'Total Trades: {trades}'); print(f'Win Rate: {agg.get(\"win_rate_pct\", 0):.1f}%%'); print(f'Net PnL: ${agg.get(\"net_pnl\", 0):.2f}'); print(f'Gross PnL: ${agg.get(\"gross_pnl\", 0):.2f}'); print(f'Fees: ${agg.get(\"total_fees\", 0):.2f}'); roi=(agg.get('net_pnl',0)/agg.get('initial_equity',1)*100) if agg.get('initial_equity',0)>0 else 0; print(f'ROI: {roi:.2f}%%'); print(); print('='*80); print('🏆 SONUÇ'); print('='*80); print(); verdict='✅ KÂRLI!' if agg.get('net_pnl',0)>0 else '⚠️ ZARARDA' if trades>0 else '❌ TRADE YOK'; print(verdict); print(); print(f'Detayli rapor: results_REAL_FINAL.json'); print('='*80)"

echo.
echo ================================================================================
echo Tamamlandi!
echo results_REAL_FINAL.json dosyasini Claude'a yukleyin.
echo ================================================================================
echo.
pause
