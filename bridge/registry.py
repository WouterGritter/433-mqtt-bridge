import threading
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
        with open(SENSORS_CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)

        config['sensors'][index]['identifier']['id'] = new_id

        with open(SENSORS_CONFIG_PATH, 'w') as f:
            yaml.safe_dump(config, f, sort_keys=False)

        sensors[index].identifier.identifier['id'] = new_id


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
