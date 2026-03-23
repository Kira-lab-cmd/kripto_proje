# File: backend/event_bus.py
"""
Event Bus - Decoupled Event-Driven Architecture

Central event dispatcher for loosely coupled communication between:
- WebSocket client
- Trading strategies
- Risk management
- Audit system
- Notification system

Features:
- Type-safe events
- Async handlers
- Priority-based dispatch
- Error isolation (one handler failure doesn't affect others)
- Event filtering and routing
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Any, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Event types in the trading system"""
    
    # Market data events
    PRICE_UPDATE = "price_update"
    OHLCV_UPDATE = "ohlcv_update"
    ORDER_BOOK_UPDATE = "order_book_update"
    
    # Trading events
    SIGNAL_GENERATED = "signal_generated"
    TRADE_REQUESTED = "trade_requested"
    TRADE_EXECUTED = "trade_executed"
    TRADE_FAILED = "trade_failed"
    
    # Position events
    POSITION_OPENED = "position_opened"
    POSITION_CLOSED = "position_closed"
    POSITION_UPDATED = "position_updated"
    
    # Strategy events
    GRID_REBALANCED = "grid_rebalanced"
    GRID_CROSSING = "grid_crossing"
    STOP_LOSS_HIT = "stop_loss_hit"
    TAKE_PROFIT_HIT = "take_profit_hit"
    
    # Risk events
    RISK_LIMIT_WARNING = "risk_limit_warning"
    RISK_LIMIT_BREACHED = "risk_limit_breached"
    RE_ENTRY_BLOCKED = "re_entry_blocked"
    
    # System events
    SYSTEM_ERROR = "system_error"
    SYSTEM_WARNING = "system_warning"
    BOT_PAUSED = "bot_paused"
    BOT_RESUMED = "bot_resumed"


class EventPriority(Enum):
    """Event handler priority levels"""
    CRITICAL = 0   # Executed first (e.g., risk checks)
    HIGH = 1       # Important (e.g., trade execution)
    NORMAL = 2     # Default priority
    LOW = 3        # Background tasks (e.g., logging)


@dataclass
class Event:
    """
    Base event class.
    
    All events must have:
    - type: EventType
    - timestamp: When event occurred (nanoseconds)
    - data: Event-specific data
    - metadata: Optional metadata
    """
    type: EventType
    timestamp: int  # nanoseconds for HFT precision
    data: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def create(cls, event_type: EventType, **data) -> 'Event':
        """
        Factory method to create events easily.
        
        Example:
            event = Event.create(
                EventType.PRICE_UPDATE,
                symbol="BTC/USDT",
                price=70000.0
            )
        """
        return cls(
            type=event_type,
            timestamp=time.time_ns(),
            data=data
        )


@dataclass
class HandlerRegistration:
    """Internal: Handler registration info"""
    handler: Callable
    priority: EventPriority
    filter_func: Optional[Callable[[Event], bool]]
    name: Optional[str]


class EventBus:
    """
    Central event bus for pub/sub communication.
    
    Features:
    - Multiple subscribers per event type
    - Priority-based handler execution
    - Error isolation (one handler failure doesn't affect others)
    - Event filtering
    - Handler statistics
    
    Example:
        bus = EventBus()
        
        @bus.subscribe(EventType.PRICE_UPDATE)
        async def on_price(event: Event):
            print(f"Price: {event.data['price']}")
        
        await bus.publish(Event.create(
            EventType.PRICE_UPDATE,
            symbol="BTC/USDT",
            price=70000.0
        ))
    """
    
    def __init__(self, max_queue_size: int = 10000):
        """
        Initialize event bus.
        
        Args:
            max_queue_size: Maximum events in queue before backpressure
        """
        # Subscribers: EventType -> List[HandlerRegistration]
        self._subscribers: Dict[EventType, List[HandlerRegistration]] = defaultdict(list)
        
        # Event queue (async)
        self._event_queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        
        # Dispatcher task
        self._dispatcher_task: Optional[asyncio.Task] = None
        self._is_running = False
        
        # Statistics
        self._stats = {
            "events_published": 0,
            "events_processed": 0,
            "events_dropped": 0,
            "handler_errors": 0
        }
        
        # Handler stats: handler_name -> {calls, errors, total_time}
        self._handler_stats: Dict[str, Dict] = defaultdict(
            lambda: {"calls": 0, "errors": 0, "total_time": 0.0}
        )
    
    def subscribe(
        self,
        event_type: EventType,
        priority: EventPriority = EventPriority.NORMAL,
        filter_func: Optional[Callable[[Event], bool]] = None,
        name: Optional[str] = None
    ):
        """
        Decorator to subscribe to an event type.
        
        Args:
            event_type: Type of event to subscribe to
            priority: Handler priority (CRITICAL executed first)
            filter_func: Optional filter (return True to process event)
            name: Optional handler name (for stats)
        
        Example:
            @bus.subscribe(EventType.PRICE_UPDATE, priority=EventPriority.HIGH)
            async def on_price(event: Event):
                if event.data['symbol'] == 'BTC/USDT':
                    # Process BTC price
                    pass
        """
        def decorator(handler: Callable):
            registration = HandlerRegistration(
                handler=handler,
                priority=priority,
                filter_func=filter_func,
                name=name or handler.__name__
            )
            
            self._subscribers[event_type].append(registration)
            
            # Sort by priority (CRITICAL first)
            self._subscribers[event_type].sort(
                key=lambda r: r.priority.value
            )
            
            logger.debug(
                f"📝 Registered handler '{registration.name}' "
                f"for {event_type.value} (priority: {priority.name})"
            )
            
            return handler
        
        return decorator
    
    async def publish(self, event: Event):
        """
        Publish an event to the bus.
        
        Events are queued and dispatched asynchronously.
        
        Args:
            event: Event to publish
        """
        self._stats["events_published"] += 1
        
        try:
            # Try to queue (non-blocking)
            self._event_queue.put_nowait(event)
        except asyncio.QueueFull:
            # Queue full - drop event (backpressure)
            self._stats["events_dropped"] += 1
            logger.warning(
                f"⚠️ Event queue full! Dropped event: {event.type.value}"
            )
    
    async def publish_and_wait(self, event: Event):
        """
        Publish event and wait for all handlers to complete.
        
        Use sparingly - blocks until all handlers finish!
        
        Args:
            event: Event to publish
        """
        await self._dispatch_event(event)
    
    async def start(self):
        """Start the event dispatcher"""
        if self._is_running:
            logger.warning("Event bus already running")
            return
        
        self._is_running = True
        self._dispatcher_task = asyncio.create_task(
            self._dispatcher_loop(),
            name="event_bus_dispatcher"
        )
        
        logger.info("✅ Event bus started")
    
    async def stop(self):
        """Stop the event dispatcher gracefully"""
        if not self._is_running:
            return
        
        logger.info("🛑 Stopping event bus...")
        
        self._is_running = False
        
        if self._dispatcher_task:
            self._dispatcher_task.cancel()
            try:
                await self._dispatcher_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ Event bus stopped")
    
    async def _dispatcher_loop(self):
        """Main event dispatcher loop"""
        logger.info("📡 Event dispatcher loop started")
        
        while self._is_running:
            try:
                # Get next event (with timeout)
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0
                )
                
                # Dispatch event to handlers
                await self._dispatch_event(event)
                
                self._stats["events_processed"] += 1
                self._event_queue.task_done()
                
            except asyncio.TimeoutError:
                # No events - normal
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Dispatcher loop error: {e}")
    
    async def _dispatch_event(self, event: Event):
        """
        Dispatch event to all registered handlers.
        
        Handlers are executed in priority order.
        Errors in one handler don't affect others.
        """
        handlers = self._subscribers.get(event.type, [])
        
        if not handlers:
            return
        
        # Execute handlers in priority order
        for registration in handlers:
            # Apply filter if specified
            if registration.filter_func:
                try:
                    if not registration.filter_func(event):
                        continue  # Skip this handler
                except Exception as e:
                    logger.error(
                        f"Filter error in {registration.name}: {e}"
                    )
                    continue
            
            # Execute handler (isolated error handling)
            await self._execute_handler(registration, event)
    
    async def _execute_handler(
        self, 
        registration: HandlerRegistration, 
        event: Event
    ):
        """Execute single handler with error isolation and timing"""
        handler_name = registration.name
        start_time = time.perf_counter()
        
        try:
            # Execute handler
            await registration.handler(event)
            
            # Update stats (success)
            elapsed = time.perf_counter() - start_time
            self._handler_stats[handler_name]["calls"] += 1
            self._handler_stats[handler_name]["total_time"] += elapsed
            
        except Exception as e:
            # Update stats (error)
            self._stats["handler_errors"] += 1
            self._handler_stats[handler_name]["errors"] += 1
            
            logger.error(
                f"❌ Handler error '{handler_name}' "
                f"for {event.type.value}: {e}"
            )
    
    def get_stats(self) -> dict:
        """Get event bus statistics"""
        return {
            "is_running": self._is_running,
            "queue_size": self._event_queue.qsize(),
            "total_subscribers": sum(
                len(handlers) 
                for handlers in self._subscribers.values()
            ),
            "events_published": self._stats["events_published"],
            "events_processed": self._stats["events_processed"],
            "events_dropped": self._stats["events_dropped"],
            "handler_errors": self._stats["handler_errors"]
        }
    
    def get_handler_stats(self) -> dict:
        """Get per-handler statistics"""
        stats = {}
        
        for name, data in self._handler_stats.items():
            calls = data["calls"]
            avg_time = (
                data["total_time"] / calls 
                if calls > 0 else 0
            )
            
            stats[name] = {
                "calls": calls,
                "errors": data["errors"],
                "avg_time_ms": avg_time * 1000,
                "success_rate": (
                    (calls - data["errors"]) / calls 
                    if calls > 0 else 0
                )
            }
        
        return stats


# ==========================================
# CONVENIENCE FUNCTIONS
# ==========================================
# Global event bus instance
_global_bus: Optional[EventBus] = None


def get_global_bus() -> EventBus:
    """Get or create global event bus"""
    global _global_bus
    
    if _global_bus is None:
        _global_bus = EventBus()
    
    return _global_bus


# ==========================================
# USAGE EXAMPLE
# ==========================================
if __name__ == "__main__":
    async def main():
        # Create event bus
        bus = EventBus()
        await bus.start()
        
        # Subscribe to price updates (HIGH priority)
        @bus.subscribe(EventType.PRICE_UPDATE, priority=EventPriority.HIGH)
        async def on_price_high(event: Event):
            print(
                f"🔴 HIGH: {event.data['symbol']} = "
                f"${event.data['price']:,.2f}"
            )
        
        # Subscribe to price updates (NORMAL priority)
        @bus.subscribe(EventType.PRICE_UPDATE, priority=EventPriority.NORMAL)
        async def on_price_normal(event: Event):
            print(
                f"🟡 NORMAL: {event.data['symbol']} = "
                f"${event.data['price']:,.2f}"
            )
        
        # Subscribe with filter (only BTC)
        @bus.subscribe(
            EventType.PRICE_UPDATE,
            priority=EventPriority.LOW,
            filter_func=lambda e: e.data.get('symbol') == 'BTC/USDT'
        )
        async def on_btc_price(event: Event):
            print(
                f"🟢 LOW (BTC only): "
                f"${event.data['price']:,.2f}"
            )
        
        # Publish events
        await bus.publish(Event.create(
            EventType.PRICE_UPDATE,
            symbol="BTC/USDT",
            price=70000.0
        ))
        
        await bus.publish(Event.create(
            EventType.PRICE_UPDATE,
            symbol="ETH/USDT",
            price=4000.0
        ))
        
        # Wait for processing
        await asyncio.sleep(1)
        
        # Print stats
        print("\n📊 Stats:", bus.get_stats())
        print("\n📈 Handler Stats:")
        for name, stats in bus.get_handler_stats().items():
            print(f"  {name}: {stats}")
        
        # Stop
        await bus.stop()
    
    # Run
    asyncio.run(main())
