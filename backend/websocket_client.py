# File: backend/websocket_client.py
"""
Binance WebSocket Client - HFT Grade
- Real-time price streaming (sub-100ms latency)
- Auto-reconnect with exponential backoff
- Heartbeat monitoring
- Event-driven architecture
"""

import asyncio
import json
import logging
import time
from typing import Callable, Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

try:
    import websockets
    from websockets.exceptions import WebSocketException
except ImportError:
    raise ImportError(
        "websockets library required: pip install websockets"
    )

logger = logging.getLogger(__name__)


class StreamType(Enum):
    """WebSocket stream types"""
    TRADE = "trade"           # Individual trades
    KLINE = "kline"           # Candlestick data
    TICKER = "ticker"         # 24hr ticker
    BOOK_TICKER = "bookTicker"  # Best bid/ask
    DEPTH = "depth"           # Order book depth


@dataclass
class PriceUpdate:
    """Price update event"""
    symbol: str
    price: float
    timestamp: int  # milliseconds
    volume: Optional[float] = None
    is_buyer_maker: Optional[bool] = None


class BinanceWebSocketClient:
    """
    Binance WebSocket client for real-time data streaming.
    
    Features:
    - Multi-symbol streaming
    - Auto-reconnect with exponential backoff
    - Heartbeat/ping-pong monitoring
    - Event handlers for different stream types
    - Thread-safe operation
    
    Example:
        ws = BinanceWebSocketClient(["BTC/USDT", "ETH/USDT"])
        
        @ws.on_price_update
        async def handle_price(update: PriceUpdate):
            print(f"{update.symbol}: ${update.price}")
        
        await ws.connect()
    """
    
    def __init__(
        self, 
        symbols: List[str],
        stream_type: StreamType = StreamType.TRADE,
        testnet: bool = False
    ):
        """
        Initialize WebSocket client.
        
        Args:
            symbols: List of trading pairs (e.g., ["BTC/USDT", "ETH/USDT"])
            stream_type: Type of stream to subscribe to
            testnet: Use testnet endpoint if True
        """
        self.symbols = symbols
        self.stream_type = stream_type
        self.testnet = testnet
        
        # Connection state
        self.ws = None
        self.is_connected = False
        self.is_running = False
        
        # Event handlers
        self._price_handlers: List[Callable] = []
        self._error_handlers: List[Callable] = []
        self._connect_handlers: List[Callable] = []
        self._disconnect_handlers: List[Callable] = []
        
        # Reconnect config
        self.reconnect_delay = 1.0  # seconds
        self.max_reconnect_delay = 60.0  # seconds
        self.reconnect_attempts = 0
        
        # Heartbeat monitoring
        self.last_message_time = 0.0
        self.heartbeat_timeout = 30.0  # seconds
        self.heartbeat_task: Optional[asyncio.Task] = None
        
        # Statistics
        self.message_count = 0
        self.error_count = 0
        self.reconnect_count = 0
        
    def on_price_update(self, handler: Callable[[PriceUpdate], None]):
        """Register price update handler"""
        self._price_handlers.append(handler)
        return handler
    
    def on_error(self, handler: Callable[[Exception], None]):
        """Register error handler"""
        self._error_handlers.append(handler)
        return handler
    
    def on_connect(self, handler: Callable[[], None]):
        """Register connect handler"""
        self._connect_handlers.append(handler)
        return handler
    
    def on_disconnect(self, handler: Callable[[], None]):
        """Register disconnect handler"""
        self._disconnect_handlers.append(handler)
        return handler
    
    def _build_websocket_url(self) -> str:
        """Build WebSocket URL for multi-stream"""
        # Convert symbols to Binance format (BTCUSDT)
        formatted_symbols = [
            s.replace("/", "").lower() 
            for s in self.symbols
        ]
        
        # Build stream names
        streams = [
            f"{symbol}@{self.stream_type.value}" 
            for symbol in formatted_symbols
        ]
        
        # WebSocket endpoint
        if self.testnet:
            base_url = "wss://testnet.binance.vision/stream"
        else:
            base_url = "wss://stream.binance.com:9443/stream"
        
        # Multi-stream URL
        url = f"{base_url}?streams={'/'.join(streams)}"
        
        return url
    
    async def connect(self):
        """
        Connect to WebSocket and start streaming.
        Auto-reconnects on disconnection.
        """
        self.is_running = True
        
        while self.is_running:
            try:
                await self._connect_and_stream()
            except asyncio.CancelledError:
                logger.info("WebSocket cancelled")
                break
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                await self._handle_error(e)
                
                # Exponential backoff
                if self.is_running:
                    await self._reconnect_backoff()
    
    async def _connect_and_stream(self):
        """Internal: Connect and process messages"""
        url = self._build_websocket_url()
        
        logger.info(f"🌐 Connecting to Binance WebSocket...")
        logger.info(f"   Symbols: {', '.join(self.symbols)}")
        logger.info(f"   Stream: {self.stream_type.value}")
        
        async with websockets.connect(
            url,
            ping_interval=20,  # Send ping every 20s
            ping_timeout=10,   # Wait 10s for pong
            close_timeout=5
        ) as ws:
            self.ws = ws
            self.is_connected = True
            self.reconnect_attempts = 0
            self.reconnect_delay = 1.0
            
            logger.info("✅ WebSocket connected")
            
            # Start heartbeat monitor
            self.heartbeat_task = asyncio.create_task(
                self._heartbeat_monitor()
            )
            
            # Notify connect handlers
            await self._dispatch_connect()
            
            # Process messages
            async for message in ws:
                try:
                    await self._process_message(message)
                except Exception as e:
                    logger.error(f"Message processing error: {e}")
                    self.error_count += 1
    
    async def _process_message(self, message: str):
        """Process incoming WebSocket message"""
        self.last_message_time = time.time()
        self.message_count += 1
        
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON: {message[:100]}")
            return
        
        # Multi-stream format: {"stream": "btcusdt@trade", "data": {...}}
        if 'stream' in data and 'data' in data:
            stream_data = data['data']
            
            # Parse based on stream type
            if self.stream_type == StreamType.TRADE:
                await self._handle_trade_update(stream_data)
            elif self.stream_type == StreamType.KLINE:
                await self._handle_kline_update(stream_data)
            elif self.stream_type == StreamType.TICKER:
                await self._handle_ticker_update(stream_data)
            elif self.stream_type == StreamType.BOOK_TICKER:
                await self._handle_book_ticker_update(stream_data)
    
    async def _handle_trade_update(self, data: dict):
        """Handle trade stream update"""
        try:
            # Parse trade data
            symbol_raw = data['s']  # BTCUSDT
            
            # Format to standard (BTC/USDT)
            if len(symbol_raw) >= 6:
                base = symbol_raw[:-4]
                quote = symbol_raw[-4:]
                symbol = f"{base}/{quote}"
            else:
                symbol = symbol_raw
            
            price = float(data['p'])
            timestamp = int(data['T'])  # Trade time
            volume = float(data['q'])   # Quantity
            is_buyer_maker = bool(data['m'])  # True if buyer is maker
            
            # Create update
            update = PriceUpdate(
                symbol=symbol,
                price=price,
                timestamp=timestamp,
                volume=volume,
                is_buyer_maker=is_buyer_maker
            )
            
            # Dispatch to handlers (fire and forget)
            await self._dispatch_price_update(update)
            
        except Exception as e:
            logger.error(f"Trade update parse error: {e}")
    
    async def _handle_kline_update(self, data: dict):
        """Handle kline (candlestick) stream update"""
        try:
            kline = data['k']
            symbol_raw = kline['s']
            
            # Format symbol
            if len(symbol_raw) >= 6:
                base = symbol_raw[:-4]
                quote = symbol_raw[-4:]
                symbol = f"{base}/{quote}"
            else:
                symbol = symbol_raw
            
            # Only process closed candles
            if kline['x']:  # Is candle closed?
                price = float(kline['c'])  # Close price
                timestamp = int(kline['T'])  # Close time
                volume = float(kline['v'])  # Volume
                
                update = PriceUpdate(
                    symbol=symbol,
                    price=price,
                    timestamp=timestamp,
                    volume=volume
                )
                
                await self._dispatch_price_update(update)
                
        except Exception as e:
            logger.error(f"Kline update parse error: {e}")
    
    async def _handle_ticker_update(self, data: dict):
        """Handle 24hr ticker stream update"""
        try:
            symbol_raw = data['s']
            
            # Format symbol
            if len(symbol_raw) >= 6:
                base = symbol_raw[:-4]
                quote = symbol_raw[-4:]
                symbol = f"{base}/{quote}"
            else:
                symbol = symbol_raw
            
            price = float(data['c'])  # Current price
            timestamp = int(data['E'])  # Event time
            volume = float(data['v'])  # 24hr volume
            
            update = PriceUpdate(
                symbol=symbol,
                price=price,
                timestamp=timestamp,
                volume=volume
            )
            
            await self._dispatch_price_update(update)
            
        except Exception as e:
            logger.error(f"Ticker update parse error: {e}")
    
    async def _handle_book_ticker_update(self, data: dict):
        """Handle book ticker (best bid/ask) update"""
        try:
            symbol_raw = data['s']
            
            # Format symbol
            if len(symbol_raw) >= 6:
                base = symbol_raw[:-4]
                quote = symbol_raw[-4:]
                symbol = f"{base}/{quote}"
            else:
                symbol = symbol_raw
            
            # Use mid price (best bid + best ask) / 2
            best_bid = float(data['b'])
            best_ask = float(data['a'])
            price = (best_bid + best_ask) / 2.0
            timestamp = int(data['u'])  # Update ID (use as timestamp)
            
            update = PriceUpdate(
                symbol=symbol,
                price=price,
                timestamp=timestamp
            )
            
            await self._dispatch_price_update(update)
            
        except Exception as e:
            logger.error(f"Book ticker update parse error: {e}")
    
    async def _dispatch_price_update(self, update: PriceUpdate):
        """Dispatch price update to all handlers (non-blocking)"""
        if not self._price_handlers:
            return
        
        # Fire and forget to all handlers
        tasks = [
            asyncio.create_task(handler(update))
            for handler in self._price_handlers
        ]
        
        # Don't await - let handlers run independently
        for task in tasks:
            task.add_done_callback(self._log_handler_error)
    
    async def _dispatch_connect(self):
        """Dispatch connect event"""
        for handler in self._connect_handlers:
            try:
                await handler()
            except Exception as e:
                logger.error(f"Connect handler error: {e}")
    
    async def _dispatch_disconnect(self):
        """Dispatch disconnect event"""
        for handler in self._disconnect_handlers:
            try:
                await handler()
            except Exception as e:
                logger.error(f"Disconnect handler error: {e}")
    
    async def _handle_error(self, error: Exception):
        """Handle error"""
        self.error_count += 1
        
        for handler in self._error_handlers:
            try:
                await handler(error)
            except Exception as e:
                logger.error(f"Error handler failed: {e}")
    
    def _log_handler_error(self, task: asyncio.Task):
        """Log handler errors"""
        try:
            task.result()
        except Exception as e:
            logger.error(f"Price handler error: {e}")
    
    async def _heartbeat_monitor(self):
        """Monitor connection health via heartbeat"""
        while self.is_connected:
            await asyncio.sleep(5)
            
            if self.last_message_time == 0:
                continue
            
            idle_time = time.time() - self.last_message_time
            
            if idle_time > self.heartbeat_timeout:
                logger.warning(
                    f"❌ Heartbeat timeout! No messages for {idle_time:.1f}s"
                )
                
                # Force reconnect
                if self.ws:
                    try:
                        await self.ws.close()
                    except:
                        pass
                
                break
    
    async def _reconnect_backoff(self):
        """Exponential backoff before reconnect"""
        self.reconnect_attempts += 1
        self.reconnect_count += 1
        
        delay = min(
            self.reconnect_delay * (2 ** (self.reconnect_attempts - 1)),
            self.max_reconnect_delay
        )
        
        logger.info(
            f"🔄 Reconnecting in {delay:.1f}s "
            f"(attempt {self.reconnect_attempts})"
        )
        
        await asyncio.sleep(delay)
    
    async def disconnect(self):
        """Gracefully disconnect"""
        logger.info("🛑 Disconnecting WebSocket...")
        
        self.is_running = False
        self.is_connected = False
        
        # Cancel heartbeat
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        
        # Close WebSocket
        if self.ws:
            try:
                await self.ws.close()
            except:
                pass
        
        await self._dispatch_disconnect()
        
        logger.info("✅ WebSocket disconnected")
    
    def get_stats(self) -> dict:
        """Get connection statistics"""
        return {
            "is_connected": self.is_connected,
            "message_count": self.message_count,
            "error_count": self.error_count,
            "reconnect_count": self.reconnect_count,
            "reconnect_attempts": self.reconnect_attempts,
            "uptime": time.time() - self.last_message_time if self.last_message_time else 0
        }


# ==========================================
# USAGE EXAMPLE
# ==========================================
if __name__ == "__main__":
    async def main():
        # Create WebSocket client
        ws = BinanceWebSocketClient(
            symbols=["BTC/USDT", "ETH/USDT"],
            stream_type=StreamType.TRADE
        )
        
        # Register price handler
        @ws.on_price_update
        async def on_price(update: PriceUpdate):
            print(
                f"📊 {update.symbol}: ${update.price:,.2f} "
                f"(vol: {update.volume:.4f})"
            )
        
        # Register connection handlers
        @ws.on_connect
        async def on_connect():
            print("✅ Connected!")
        
        @ws.on_disconnect
        async def on_disconnect():
            print("❌ Disconnected!")
        
        @ws.on_error
        async def on_error(error: Exception):
            print(f"⚠️ Error: {error}")
        
        # Connect and stream
        try:
            await ws.connect()
        except KeyboardInterrupt:
            await ws.disconnect()
    
    # Run
    asyncio.run(main())
