"""Detects sensors that 'ping-pong' between two value levels — the tell-tale sign that
two physically distinct 433 MHz sensors are transmitting under the same random `id` and
are therefore both being published to one configured sensor's topic (an id collision).

Why this happens: id-bearing sensors pick a fresh random `id` (0-255) on every battery
swap. The claim feature (see registry.claim_sensor) maps that id onto a configured
sensor, but two sensors can end up sharing an id, in which case their readings interleave
on the same topic and the published value flips back and forth between them.

How we tell a collision apart from a single sensor's natural variation:

* A single physical sensor drifts *smoothly* — consecutive readings are close and
  correlated (a temperature can't jump several degrees and back within a minute).
* Two interleaved sensors produce a series that repeatedly flips between two
  well-separated value levels from one message to the next.

So, per (sensor, numeric reading) we keep a short sliding window and require, together:

1. Enough messages in the window. This doubles as the frequency gate: a sensor that
   reports too rarely to judge (flaky temp sensors, TPMS, lightning) never accumulates
   MIN_SAMPLES within WINDOW_SECONDS and is silently skipped — exactly as wanted, since
   over a long enough gap two coincidental values aren't evidence of anything.
2. The values split into two well-separated clusters (median split; the gap between the
   cluster means is both large in absolute terms — which kills quantization dither — and
   large relative to the spread within each cluster). This is what makes two sensors that
   *start out* reading similarly only trip the detector once they've drifted apart.
3. The series alternates between those clusters far more than a single, smoothly varying
   source ever would. We quantify this with the Wald-Wolfowitz runs test: a real sensor
   yields *few* runs (long stretches on one side of the median, i.e. negative serial
   correlation, z < 0), while a ping-pong yields *many* runs (a large positive z). This
   is also what keeps a genuine trend (all-low then all-high) from firing — a trend is
   few long runs, not rapid alternation.

Only sensors whose identifier carries an `id` are considered (buttons/doors send a stable
raw code and can't collide). Alerts are rate-limited per sensor since fixing a collision
requires physical intervention (re-pairing / re-claiming).
"""

import json
import statistics
import threading
import time
from collections import deque
from math import sqrt
from typing import Any, Optional, TYPE_CHECKING

from . import events
from .config import (
    MONITOR_COOLDOWN_SECONDS,
    MONITOR_ENABLED,
    MONITOR_MIN_AMPLITUDE,
    MONITOR_MIN_SAMPLES,
    MONITOR_RUNS_Z,
    MONITOR_SEPARATION_RATIO,
    MONITOR_WINDOW_SECONDS,
)
from .notifications import send_discord_message

if TYPE_CHECKING:
    from .sensors import RadioSensor, Reading

# Reading values come through as strings/ints/floats; only continuously-valued numeric
# readings can ping-pong meaningfully, so non-numeric ones (door state, button press) are
# dropped here regardless of sensor type.
_EPSILON = 1e-9

_lock = threading.Lock()


class _SensorMonitor:
    """Sliding windows of recent numeric readings for one sensor (keyed by topic), plus
    the time of its last alert for rate-limiting."""

    def __init__(self, key: str):
        self.key = key
        self.series: dict[str, deque[tuple[float, float]]] = {}
        self.last_alert_ts: float = 0.0

    def add(self, topic: str, value: float, ts: float) -> None:
        window = self.series.get(topic)
        if window is None:
            window = deque()
            self.series[topic] = window
        window.append((ts, value))
        cutoff = ts - MONITOR_WINDOW_SECONDS
        while window and window[0][0] < cutoff:
            window.popleft()


_monitors: dict[str, _SensorMonitor] = {}


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _runs_z(signs: list[bool]) -> Optional[float]:
    """Wald-Wolfowitz runs-test z-score for a binary (above/below median) sequence.

    Returns (observed_runs - expected_runs) / std. A large positive value means the
    sequence alternates far more than chance (the ping-pong signature); a single,
    autocorrelated sensor gives a negative value. None if the test is undefined (one
    side empty, or zero variance)."""
    n1 = sum(signs)
    n2 = len(signs) - n1
    n = n1 + n2
    if n1 < 1 or n2 < 1:
        return None

    runs = 1 + sum(1 for a, b in zip(signs, signs[1:]) if a != b)
    expected = 1 + (2 * n1 * n2) / n
    variance = (2 * n1 * n2 * (2 * n1 * n2 - n)) / (n * n * (n - 1))
    if variance <= 0:
        return None
    return (runs - expected) / sqrt(variance)


def _evaluate(window: deque[tuple[float, float]]) -> Optional[dict[str, Any]]:
    """Decide whether a single topic's window shows a ping-pong. Returns a detail dict
    when all gates pass, else None."""
    if len(window) < MONITOR_MIN_SAMPLES:
        return None

    values = [value for _, value in window]
    median = statistics.median(values)

    # Median split. Ties go to the low side so a steady sensor (every value == median)
    # lands entirely on one side and is rejected below for having an empty cluster.
    signs = [value > median for value in values]
    high = [value for value in values if value > median]
    low = [value for value in values if value <= median]
    if len(high) < 2 or len(low) < 2:
        return None

    # Alternation gate (the strong discriminator): many runs => rapid flipping.
    z = _runs_z(signs)
    if z is None or z < MONITOR_RUNS_Z:
        return None

    # Separation gate: the two levels must be far apart, both absolutely (so quantization
    # dither doesn't count) and relative to each cluster's own spread (so a single noisy
    # sensor straddling the median doesn't count). With near-zero within-cluster spread
    # (two clean constant levels) the ratio test is moot and the absolute floor decides.
    gap = statistics.fmean(high) - statistics.fmean(low)
    within = sqrt((statistics.pvariance(high) + statistics.pvariance(low)) / 2)
    if gap < MONITOR_MIN_AMPLITUDE:
        return None
    if within > _EPSILON and gap < MONITOR_SEPARATION_RATIO * within:
        return None

    flips = sum(1 for a, b in zip(signs, signs[1:]) if a != b)
    return {
        'low_level': round(statistics.fmean(low), 3),
        'high_level': round(statistics.fmean(high), 3),
        'amplitude': round(gap, 3),
        'flip_rate': round(flips / (len(signs) - 1), 3),
        'runs_z': round(z, 2),
        'samples': len(values),
        'span_seconds': round(window[-1][0] - window[0][0], 1),
    }


def record(sensor: 'RadioSensor', readings: list['Reading'], receive_time) -> None:
    """Feed a known sensor's just-published readings to the monitor and alert if a
    ping-pong is detected. Called from the packet-processing worker for non-duplicate
    packets only. Never raises into the caller."""
    if not MONITOR_ENABLED:
        return
    # Only id-bearing sensors can suffer a random-id collision; buttons/doors send a
    # stable code and are unaffected.
    if 'id' not in sensor.identifier.identifier:
        return

    ts = receive_time.timestamp()
    key = sensor.topic_prefix

    with _lock:
        monitor = _monitors.get(key)
        if monitor is None:
            monitor = _SensorMonitor(key)
            _monitors[key] = monitor

        detections: dict[str, dict[str, Any]] = {}
        for reading in readings:
            value = _to_float(reading.value)
            if value is None:
                continue
            monitor.add(reading.topic, value, ts)
            detail = _evaluate(monitor.series[reading.topic])
            if detail is not None:
                detections[reading.topic] = detail

        if not detections:
            return

        now = time.time()
        if now - monitor.last_alert_ts < MONITOR_COOLDOWN_SECONDS:
            return
        monitor.last_alert_ts = now

    # Report the most pronounced topic (largest runs-z) when several flag at once.
    topic, detail = max(detections.items(), key=lambda item: item[1]['runs_z'])
    _alert(key, topic, detail)


def _alert(key: str, topic: str, detail: dict[str, Any]) -> None:
    payload = {'sensor': key, 'topic': topic, **detail}
    print(f'Possible sensor id collision (ping-pong) on {key}: {json.dumps(payload)}')

    message = (
        f'**Possible sensor ID collision on `{key}`** :twisted_rightwards_arrows:\n'
        f'`{topic}` is ping-ponging between **{detail["low_level"]}** and '
        f'**{detail["high_level"]}** — likely two different sensors sharing the same '
        f'random `id`. Re-pair / re-claim the sensor, or check for a colliding device.\n'
        f'```json\n{json.dumps(payload, indent=2)}\n```'
    )
    send_discord_message(message)
    events.emit('collision', payload)


def drop_sensor(key: str) -> None:
    """Discard a sensor's monitor state (e.g. when it is removed/retopic'd via the
    dashboard), mirroring stats.drop_sensor."""
    with _lock:
        _monitors.pop(key, None)
