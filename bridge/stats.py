"""In-memory live statistics for sensors and receivers.

These are runtime stats (rates, last values, signal, battery, receiver health) that the
dashboard shows and that the event bus pushes on every packet. They are intentionally
not persisted — like `last_seen` they reset on restart; reading *history* lives in
`storage` (SQLite). Sensor stats are keyed by `topic_prefix`, which is stable across a
battery/id swap (the whole point of the claim feature), so a sensor's stats survive
being re-claimed.

All access is guarded by a single lock since updates come from the packet-processing
thread and the receiver threads, while reads come from the web server's thread.
"""

import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .packet import Packet
    from .sensors import RadioSensor, Reading

# Window (seconds) over which packets-per-minute and average signal are computed.
_RATE_WINDOW = 60.0

_lock = threading.Lock()


def _trim(timestamps: deque, now: float) -> None:
    while timestamps and now - timestamps[0] > _RATE_WINDOW:
        timestamps.popleft()


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class SensorStats:
    def __init__(self, key: str):
        self.key = key
        self.packet_count = 0
        self.last_seen: Optional[datetime] = None
        self.last_readings: dict[str, Any] = {}
        self.last_raw: dict[str, Any] = {}
        self.battery_ok: Optional[bool] = None
        self.rssi: Optional[float] = None
        self.snr: Optional[float] = None
        self._recent: deque[float] = deque()

    def record(self, packet: 'Packet', readings: list['Reading']) -> None:
        now = time.time()
        self.packet_count += 1
        self.last_seen = packet.receive_time
        self.last_raw = packet.data
        self.last_readings = {reading.topic: reading.value for reading in readings}

        # Carry forward the previous value when a field is absent from this packet.
        if 'battery_ok' in packet.data:
            self.battery_ok = bool(packet.data['battery_ok'])
        if 'rssi' in packet.data:
            self.rssi = _to_float(packet.data['rssi'])
        if 'snr' in packet.data:
            self.snr = _to_float(packet.data['snr'])

        self._recent.append(now)
        _trim(self._recent, now)

    def snapshot(self) -> dict[str, Any]:
        now = time.time()
        _trim(self._recent, now)
        return {
            'key': self.key,
            'packet_count': self.packet_count,
            'last_seen': self.last_seen.isoformat() if self.last_seen else None,
            'seconds_since_seen': (datetime.now() - self.last_seen).total_seconds() if self.last_seen else None,
            'rate_per_min': len(self._recent),
            'last_readings': self.last_readings,
            'last_raw': self.last_raw,
            'battery_ok': self.battery_ok,
            'rssi': self.rssi,
            'snr': self.snr,
        }


class ReceiverStats:
    def __init__(self, name: str):
        self.name = name
        self.running = False
        self.restart_count = 0
        self.first_packet = False
        self.packet_count = 0
        self.last_seen: Optional[datetime] = None
        self._recent: deque[float] = deque()
        self._recent_rssi: deque[tuple[float, float]] = deque()

    def record_packet(self, packet: 'Packet') -> None:
        now = time.time()
        self.packet_count += 1
        self.first_packet = True
        self.last_seen = packet.receive_time
        self._recent.append(now)
        _trim(self._recent, now)

        rssi = _to_float(packet.data.get('rssi'))
        if rssi is not None:
            self._recent_rssi.append((now, rssi))
            while self._recent_rssi and now - self._recent_rssi[0][0] > _RATE_WINDOW:
                self._recent_rssi.popleft()

    def snapshot(self) -> dict[str, Any]:
        now = time.time()
        _trim(self._recent, now)
        avg_rssi = None
        if self._recent_rssi:
            avg_rssi = sum(rssi for _, rssi in self._recent_rssi) / len(self._recent_rssi)
        return {
            'name': self.name,
            'running': self.running,
            'restart_count': self.restart_count,
            'first_packet': self.first_packet,
            'packet_count': self.packet_count,
            'last_seen': self.last_seen.isoformat() if self.last_seen else None,
            'rate_per_min': len(self._recent),
            'avg_rssi': avg_rssi,
        }


_sensor_stats: dict[str, SensorStats] = {}
_receiver_stats: dict[str, ReceiverStats] = {}


def _get_sensor(key: str) -> SensorStats:
    stats = _sensor_stats.get(key)
    if stats is None:
        stats = SensorStats(key)
        _sensor_stats[key] = stats
    return stats


def _get_receiver(name: str) -> ReceiverStats:
    stats = _receiver_stats.get(name)
    if stats is None:
        stats = ReceiverStats(name)
        _receiver_stats[name] = stats
    return stats


# --- mutations -------------------------------------------------------------

def record_sensor_packet(sensor: 'RadioSensor', packet: 'Packet', readings: list['Reading']) -> None:
    with _lock:
        _get_sensor(sensor.topic_prefix).record(packet, readings)


def record_receiver_packet(packet: 'Packet') -> None:
    with _lock:
        _get_receiver(packet.origin.name).record_packet(packet)


def ensure_receiver(name: str) -> None:
    with _lock:
        _get_receiver(name)


def set_receiver_running(name: str, running: bool) -> None:
    with _lock:
        _get_receiver(name).running = running


def mark_receiver_restart(name: str) -> None:
    with _lock:
        _get_receiver(name).restart_count += 1


def drop_sensor(key: str) -> None:
    """Discard a sensor's stats (e.g. when it is removed via the dashboard)."""
    with _lock:
        _sensor_stats.pop(key, None)


# --- reads -----------------------------------------------------------------

def sensor_snapshot(key: str) -> Optional[dict[str, Any]]:
    with _lock:
        stats = _sensor_stats.get(key)
        return stats.snapshot() if stats else None


def all_sensor_snapshots() -> dict[str, dict[str, Any]]:
    with _lock:
        return {key: stats.snapshot() for key, stats in _sensor_stats.items()}


def receiver_snapshot(name: str) -> Optional[dict[str, Any]]:
    with _lock:
        stats = _receiver_stats.get(name)
        return stats.snapshot() if stats else None


def all_receiver_snapshots() -> dict[str, dict[str, Any]]:
    with _lock:
        return {name: stats.snapshot() for name, stats in _receiver_stats.items()}
