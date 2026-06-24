from queue import Queue
from typing import Optional, TYPE_CHECKING

import yaml

from . import receiver as receiver_module
from .config import RECEIVERS_CONFIG_PATH, SENSORS_CONFIG_PATH
from .packet import Packet
from .sensors import RadioSensor, SensorIdentifier, build_sensor

if TYPE_CHECKING:
    from .receiver import Receiver

custom_decoders: list[str] = []

receivers: list['Receiver'] = []

sensors: list[RadioSensor] = []
ignored_sensors: list[SensorIdentifier] = []

packet_receive_queue: Queue[Packet] = Queue()


def find_sensor(packet: Packet) -> Optional[RadioSensor]:
    for sensor in sensors:
        if sensor.matches(packet):
            return sensor

    return None


def is_ignored(packet: Packet) -> bool:
    for ignored in ignored_sensors:
        if ignored.matches(packet):
            return True

    return False


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
