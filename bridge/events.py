"""A tiny synchronous-to-async event bus.

The packet pipeline runs in plain threads (receivers, the processing worker) while the
web server runs on asyncio. This bus lets the threaded side push events to WebSocket
clients without knowing anything about asyncio: the web server registers its running
loop once at startup, and `emit()` (safe to call from any thread) fans each event out to
every connected subscriber's queue via `loop.call_soon_threadsafe`.
"""

import asyncio
import threading
from typing import Any, Optional

# The web server's event loop, captured at startup. Until it is set, emit() is a no-op
# (events produced before the web server is up have nowhere to go).
_loop: Optional[asyncio.AbstractEventLoop] = None

# Per-subscriber queues. Guarded by _lock because subscribers are added/removed on the
# event loop thread while emit() iterates from packet-processing threads.
_subscribers: set['asyncio.Queue[dict[str, Any]]'] = set()
_lock = threading.Lock()

# Bound the per-subscriber backlog so a slow/stuck client can't grow memory without
# limit. When full we drop the oldest event for that client.
_MAX_QUEUE = 1000


def set_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Register the web server's event loop. Call once, from that loop's thread."""
    global _loop
    _loop = loop


def subscribe() -> 'asyncio.Queue[dict[str, Any]]':
    """Register a new subscriber and return its queue. Call from the event loop."""
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=_MAX_QUEUE)
    with _lock:
        _subscribers.add(queue)
    return queue


def unsubscribe(queue: 'asyncio.Queue[dict[str, Any]]') -> None:
    """Remove a subscriber. Call from the event loop."""
    with _lock:
        _subscribers.discard(queue)


def emit(event_type: str, payload: dict[str, Any]) -> None:
    """Broadcast an event to all subscribers. Safe to call from any thread."""
    loop = _loop
    if loop is None:
        return

    event = {'type': event_type, **payload}

    with _lock:
        subscribers = list(_subscribers)

    for queue in subscribers:
        loop.call_soon_threadsafe(_offer, queue, event)


def _offer(queue: 'asyncio.Queue[dict[str, Any]]', event: dict[str, Any]) -> None:
    """Enqueue an event, dropping the oldest if the subscriber is full. Runs on the loop."""
    if queue.full():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
    try:
        queue.put_nowait(event)
    except asyncio.QueueFull:
        pass
