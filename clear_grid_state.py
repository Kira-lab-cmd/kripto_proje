"""
BTC ve ETH grid state'ini DB'den temizler.
Çalıştırmadan önce botu durdur!

Kullanım:
  python clear_grid_state.py

Sonra botu yeniden başlat → BTC/ETH grid güncel fiyattan sıfırdan init olur.
SOL grid state korunur.
"""
import sqlite3
import os

# DB yolunu ayarla
DB_PATH = os.path.join(os.path.dirname(__file__), "backend", "trading_bot.db")
if not os.path.exists(DB_PATH):
    # Alternatif yol dene
    DB_PATH = "backend/trading_bot.db"

print(f"DB: {DB_PATH}")
if not os.path.exists(DB_PATH):
    print("HATA: DB bulunamadı. Scripti proje klasöründen çalıştır.")
    exit(1)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Mevcut grid state'leri göster
print("\n=== MEVCUT GRID STATE'LER ===")
cur.execute("SELECT symbol, updated_at FROM grid_state ORDER BY symbol")
rows = cur.fetchall()
for sym, upd in rows:
    print(f"  {sym}: updated_at={upd}")

# BTC ve ETH grid state sil
symbols_to_clear = ["BTC/USDT", "ETH/USDT"]
print(f"\n{symbols_to_clear} grid state siliniyor...")
for sym in symbols_to_clear:
    cur.execute("DELETE FROM grid_state WHERE symbol = ?", (sym,))
    print(f"  ✅ {sym} grid state silindi")

conn.commit()

# Sonucu doğrula
print("\n=== SONRASI ===")
cur.execute("SELECT symbol, updated_at FROM grid_state ORDER BY symbol")
rows = cur.fetchall()
if rows:
    for sym, upd in rows:
        print(f"  {sym}: updated_at={upd}")
else:
    print("  (tüm grid state'ler temizlendi)")

conn.close()
print("\nHazır. Şimdi botu yeniden başlat.")
