#!/usr/bin/env python3
"""
Crypto Trading Bot - Otomatik Saatlik Log Rotation
Her saat başı otomatik yeni log dosyası oluşturur
"""
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import uvicorn

# Log klasörünü oluştur
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

# Root logger'ı temizle
root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Console handler (ekrana yazdır)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s', datefmt='%H:%M:%S')
console_handler.setFormatter(console_formatter)

# Saatlik rotating file handler
# when='H': Her saat başı rotate
# interval=1: Her 1 saatte
# backupCount=168: Son 1 haftalık logları tut (24*7=168 saat)
file_handler = TimedRotatingFileHandler(
    filename='logs/bot.log',
    when='H',           # Hourly rotation
    interval=1,         # Every 1 hour
    backupCount=168,    # Keep 7 days (168 hours)
    encoding='utf-8'
)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s', datefmt='%H:%M:%S')
file_handler.setFormatter(file_formatter)

# Custom suffix for rotated files (adds timestamp)
file_handler.suffix = "%Y-%m-%d_%H"

# Root logger'a handlers ekle
root_logger.setLevel(logging.INFO)
root_logger.addHandler(console_handler)
root_logger.addHandler(file_handler)

# Startup mesajı
logging.info("=" * 60)
logging.info("🤖 CRYPTO TRADING BOT BAŞLATILIYOR")
logging.info("📊 Log dosyaları: logs/bot.log.YYYY-MM-DD_HH")
logging.info("🔄 Otomatik saatlik rotation: AKTIF")
logging.info("💾 7 günlük log saklanacak")
logging.info("=" * 60)

# Backend main'i import et ve çalıştır
if __name__ == "__main__":
    try:
        logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
        # backend.main modülünü import et
        from backend.main import app
        
        # Uvicorn ile başlat
        uvicorn.run(
            app, 
            host="0.0.0.0", 
            port=8000, 
            reload=False, 
            log_level="info",
            log_config=None,  # Kendi logging config'imizi kullan
            access_log=False
        )
    except KeyboardInterrupt:
        logging.info("")
        logging.info("🛑 Bot kullanıcı tarafından durduruldu (Ctrl+C)")
        logging.info("=" * 60)
    except Exception as e:
        logging.error(f"❌ HATA: {e}", exc_info=True)
        sys.exit(1)