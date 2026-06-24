import threading
import time
import uuid
from collections import OrderedDict
from queue import Queue
from typing import Any, Optional, TYPE_CHECKING

import yaml

from . import events
from . import receiver as receiver_module
from .config import RECEIVERS_CONFIG_PATH, SENSORS_CONFIG_PATH
from .packet import Packet
from .sensors import RadioSensor, SensorIdentifier, build_sensor

if TYPE_CHECKING:
    from .receiver import Receiver

# Identifying fields of a device (as opposed to volatile measurements). Used to collapse
# repeated readings from the same unknown device into a single most-recent entry.
_DEVICE_FIELDS = ('model', 'subtype', 'id', 'channel', 'type')

# Set when the process is shutting down. Long-running worker loops watch this to exit
# cleanly (stop restarting receivers, stop draining the queue) instead of being killed.
shutdown_event = threading.Event()

# Long-running worker threads (receivers, the packet processor) registered here so the
# shutdown sequence can join them.
background_threads: list[threading.Thread] = []

custom_decoders: list[str] = []

receivers: list['Receiver'] = []

sensors: list[RadioSensor] = []
ignored_sensors: list[SensorIdentifier] = []

# Most recent reading per unknown (unconfigured, non-ignored) device, oldest first.
# Surfaced in the dashboard so a device can be claimed without waiting for a Discord
# alert. Keyed by device signature so one chatty device doesn't crowd out the rest.
MAX_RECENT_UNKNOWNS = 50
recent_unknowns: 'OrderedDict[str, dict[str, Any]]' = OrderedDict()
_unknowns_lock = threading.Lock()


def _device_signature(packet_data: dict[str, Any]) -> str:
    present = {key: packet_data[key] for key in _DEVICE_FIELDS if key in packet_data}
    # Fall back to the whole packet for devices that expose none of the usual id fields.
    return str(present) if present else str(sorted(packet_data.items()))


def record_unknown(packet: Packet) -> None:
    """Record a packet from an unknown device and notify dashboard clients."""
    entry = {
        'signature': _device_signature(packet.data),
        'data': packet.data,
        'receiver': packet.origin.name,
        'time': packet.receive_time.isoformat(),
    }
    with _unknowns_lock:
        recent_unknowns.pop(entry['signature'], None)
        recent_unknowns[entry['signature']] = entry
        while len(recent_unknowns) > MAX_RECENT_UNKNOWNS:
            recent_unknowns.popitem(last=False)

    events.emit('unknown', entry)


def list_recent_unknowns() -> list[dict[str, Any]]:
    """Most recent unknown readings, newest first."""
    with _unknowns_lock:
        return list(reversed(recent_unknowns.values()))

# Guards reads of and mutations to `sensors` / `ignored_sensors`. The packet-processing
# thread reads these lists while the web server may mutate them (claim, add, remove,
# modify), so all such access must be serialised. Reentrant so a locked mutation can call
# locked read helpers.
lock = threading.RLock()

packet_receive_queue: Queue[Packet] = Queue()


def find_sensor(packet: Packet) -> Optional[RadioSensor]:
    with lock:
        for sensor in sensors:
            if sensor.matches(packet):
                return sensor

    return None


def is_ignored(packet: Packet) -> bool:
    with lock:
        for ignored in ignored_sensors:
            if ignored.matches(packet):
                return True

    return False


def find_claim_candidates(packet_data: dict[str, any]) -> list[tuple[int, RadioSensor]]:
    """Configured sensors this packet could be claimed as: those with an `id` in their
    identifier whose every other identifier field (e.g. `model`, `channel`) matches the
    packet. The returned index is the sensor's position in both `sensors` and the
    `sensors.yml` `sensors:` list (they are loaded in lockstep)."""
    candidates = []
    with lock:
        for index, sensor in enumerate(sensors):
            identifier = sensor.identifier.identifier
            if 'id' not in identifier:
                continue
            if all(packet_data.get(key) == value for key, value in identifier.items() if key != 'id'):
                candidates.append((index, sensor))

    return candidates


def claim_sensor(index: int, new_id: any) -> None:
    """Re-point the configured sensor at `index` to `new_id`: persist it to sensors.yml
    and update the live sensor in place (no restart, runtime state preserved)."""
    with lock:
        config = _read_sensors_config()
        config['sensors'][index]['identifier']['id'] = new_id
        _write_sensors_config(config)
        sensors[index].identifier.identifier['id'] = new_id


# --- sensors.yml CRUD ------------------------------------------------------
#
# `sensors` and `ignored_sensors` are kept index-aligned with the `sensors:` /
# `ignored_sensors:` lists in sensors.yml (claim_sensor / find_claim_candidates rely on
# this), so every mutation updates both the YAML file and the in-memory list at the same
# index, under `lock`.

def _read_sensors_config() -> dict:
    with open(SENSORS_CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f) or {}
    config.setdefault('sensors', [])
    config.setdefault('ignored_sensors', [])
    return config


def _write_sensors_config(config: dict) -> None:
    with open(SENSORS_CONFIG_PATH, 'w') as f:
        yaml.safe_dump(config, f, sort_keys=False)


def add_sensor(sensor_config: dict) -> str:
    """Validate and append a sensor. Returns the new sensor's key (topic_prefix)."""
    new_sensor = build_sensor(sensor_config)  # raises on invalid config
    with lock:
        config = _read_sensors_config()
        config['sensors'].append(sensor_config)
        _write_sensors_config(config)
        sensors.append(new_sensor)
    return new_sensor.topic_prefix


def update_sensor(index: int, sensor_config: dict) -> tuple[str, str]:
    """Validate and replace the sensor at `index`. Returns (old_key, new_key). Runtime
    state (last_seen) is carried over when the topic is unchanged."""
    new_sensor = build_sensor(sensor_config)  # raises on invalid config
    with lock:
        config = _read_sensors_config()
        if not 0 <= index < len(config['sensors']):
            raise IndexError('sensor index out of range')
        old_sensor = sensors[index]
        if old_sensor.topic_prefix == new_sensor.topic_prefix:
            new_sensor.last_seen = old_sensor.last_seen
        config['sensors'][index] = sensor_config
        _write_sensors_config(config)
        sensors[index] = new_sensor
    return old_sensor.topic_prefix, new_sensor.topic_prefix


def remove_sensor(index: int) -> str:
    """Remove the sensor at `index`. Returns the removed sensor's key."""
    with lock:
        config = _read_sensors_config()
        if not 0 <= index < len(config['sensors']):
            raise IndexError('sensor index out of range')
        del config['sensors'][index]
        _write_sensors_config(config)
        removed = sensors.pop(index)
    return removed.topic_prefix


def add_ignored_sensor(identifier: dict) -> None:
    with lock:
        config = _read_sensors_config()
        config['ignored_sensors'].append(identifier)
        _write_sensors_config(config)
        ignored_sensors.append(SensorIdentifier(identifier))


def remove_ignored_sensor(index: int) -> None:
    with lock:
        config = _read_sensors_config()
        if not 0 <= index < len(config['ignored_sensors']):
            raise IndexError('ignored sensor index out of range')
        del config['ignored_sensors'][index]
        _write_sensors_config(config)
        del ignored_sensors[index]


def list_ignored_sensors() -> list[dict[str, Any]]:
    with lock:
        return [dict(ignored.identifier) for ignored in ignored_sensors]


def get_sensor_configs() -> list[dict]:
    """The raw `sensors:` entries from sensors.yml, index-aligned with `sensors`. Used to
    prefill the dashboard's edit form with the full original config."""
    with lock:
        return _read_sensors_config()['sensors']


# --- test sensors ----------------------------------------------------------
#
# Ephemeral, in-memory-only sensors used to try out a candidate config against live
# traffic. They are matched against every packet and their parsed output is emitted to
# the dashboard, but they are never published to MQTT, never persisted, and never added
# to `sensors`. Each carries a TTL so abandoned test sessions self-clean.

TEST_SENSOR_TTL = 600.0

_test_sensors: dict[str, dict[str, Any]] = {}
_test_lock = threading.Lock()


def add_test_sensor(sensor_config: dict) -> dict[str, Any]:
    """Validate and register a test sensor. Returns {id, expires_at}."""
    sensor = build_sensor(sensor_config)  # raises on invalid config
    test_id = uuid.uuid4().hex
    expires_at = time.time() + TEST_SENSOR_TTL
    with _test_lock:
        _test_sensors[test_id] = {'id': test_id, 'sensor': sensor, 'config': sensor_config, 'expires_at': expires_at}
    return {'id': test_id, 'expires_at': expires_at}


def remove_test_sensor(test_id: str) -> None:
    with _test_lock:
        _test_sensors.pop(test_id, None)


def renew_test_sensor(test_id: str) -> bool:
    """Extend a test sensor's TTL (heartbeat from the dashboard). False if it's gone."""
    with _test_lock:
        entry = _test_sensors.get(test_id)
        if entry is None:
            return False
        entry['expires_at'] = time.time() + TEST_SENSOR_TTL
        return True


def active_test_sensors() -> list[dict[str, Any]]:
    """Currently-live test sensors, pruning any that have expired."""
    now = time.time()
    with _test_lock:
        for test_id in [tid for tid, entry in _test_sensors.items() if entry['expires_at'] < now]:
            del _test_sensors[test_id]
        return list(_test_sensors.values())


def load_sensors_config():
    with open(SENSORS_CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)

    for sensor in config['sensors']:
        sensors.append(build_sensor(sensor))

    for sensor_identifier in config['ignored_sensors']:
        ignored_sensors.append(SensorIdentifier(sensor_identifier))


def load_receivers_config():
    with open(RECEIVERS_CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)

    for custom_decoder in config['custom_decoders']:
        custom_decoders.append(custom_decoder)

    for receiver_config in config['receivers']:
        receivers.append(receiver_module.Receiver(
            name=receiver_config['name'],
            arguments=receiver_config['arguments'],
        ))


# --- receivers.yml custom decoders -----------------------------------------
#
# Custom decoders are rtl_433 `-X` spec strings. They are read once at startup and passed
# to every receiver on launch, so changes here are persisted to receivers.yml and applied
# to the in-memory list immediately but only take effect for a receiver once it is
# restarted (see Receiver.restart).

def _read_receivers_config() -> dict:
    with open(RECEIVERS_CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f) or {}
    config.setdefault('receivers', [])
    config.setdefault('custom_decoders', [])
    return config


def _write_receivers_config(config: dict) -> None:
    with open(RECEIVERS_CONFIG_PATH, 'w') as f:
        yaml.safe_dump(config, f, sort_keys=False)


def list_custom_decoders() -> list[str]:
    with lock:
        return list(custom_decoders)


def add_custom_decoder(decoder: str) -> None:
    decoder = decoder.strip()
    if not decoder:
        raise ValueError('decoder must not be empty')
    with lock:
        config = _read_receivers_config()
        config['custom_decoders'].append(decoder)
        _write_receivers_config(config)
        custom_decoders.append(decoder)


def remove_custom_decoder(index: int) -> None:
    with lock:
        config = _read_receivers_config()
        if not 0 <= index < len(config['custom_decoders']):
            raise IndexError('decoder index out of range')
        del config['custom_decoders'][index]
        _write_receivers_config(config)
        del custom_decoders[index]
