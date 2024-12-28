import os
import subprocess
import json
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from queue import Queue
from typing import Optional, Callable
import yaml
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from discord_webhook import DiscordWebhook

load_dotenv()

RECEIVERS_CONFIG_PATH = os.getenv('RECEIVERS_CONFIG_PATH', 'receivers.yml')
SENSORS_CONFIG_PATH = os.getenv('SENSORS_CONFIG_PATH', 'sensors.yml')
LEGACY_RTL_433_ARGS = os.getenv('RTL_433_ARGS')
MQTT_BROKER_ADDRESS = os.getenv('MQTT_BROKER_ADDRESS', 'localhost')
MQTT_BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))
MQTT_QOS = int(os.getenv('MQTT_QOS', '0'))
MQTT_RETAIN = os.getenv('MQTT_RETAIN', 'false') == 'true'
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')

IGNORE_DATA_KEYS = [
    'repeat',
]


class Packet:
    def __init__(self, data: dict[str, any], receive_time: datetime):
        self.data = data
        self.receive_time = receive_time

    def is_duplicate(self, other: Optional['Packet'], max_time_delta: Optional[float] = None) -> bool:
        if other is None:
            return False

        if max_time_delta is not None:
            time_delta = (self.receive_time - other.receive_time).total_seconds()
            if abs(time_delta) > max_time_delta:
                return False

        return self.data == other.data


class PacketTimeRingBuffer:
    def __init__(self, max_age: float):
        self.max_age = max_age

        self.packets: list[Packet] = []

    def cleanup(self):
        now = datetime.now()
        self.packets = [packet for packet in self.packets if (now - packet.receive_time).total_seconds() < self.max_age]

    def add(self, packet: Packet):
        self.packets.append(packet)
        self.cleanup()

    def contains_duplicate(self, packet: Packet) -> bool:
        self.cleanup()
        for other in self.packets:
            if packet.is_duplicate(other):
                return True

        return False


class SensorIdentifier:
    def __init__(self, identifier: dict[str, any]):
        self.identifier = identifier

    def matches(self, packet: Packet) -> bool:
        for key, value in self.identifier.items():
            if packet.data.get(key) != value:
                return False

        return True


class RadioSensor(ABC):
    def __init__(self, topic_prefix: str, identifier: SensorIdentifier):
        self.topic_prefix = topic_prefix
        self.identifier = identifier

    def matches(self, packet: Packet) -> bool:
        return self.identifier.matches(packet)

    @abstractmethod
    def process(self, packet: Packet) -> None:
        pass


class TemperatureRadioSensor(RadioSensor):
    def __init__(self, topic_prefix: str, identifier: SensorIdentifier):
        super().__init__(topic_prefix, identifier)

    def process(self, packet: Packet) -> None:
        data_key_map = {
            'temperature': 'temperature_C',
            'humidity': 'humidity',
        }

        data = {mqtt_attribute: packet.data[data_key] for mqtt_attribute, data_key in data_key_map.items() if data_key in packet.data}
        for attribute, value in data.items():
            topic = f'{self.topic_prefix}/{attribute}'
            mqttc.publish(topic, value, qos=MQTT_QOS, retain=MQTT_RETAIN)


class ButtonRadioSensor(RadioSensor):
    def __init__(self, topic_prefix: str, identifier: SensorIdentifier, buttons: dict[str, str]):
        super().__init__(topic_prefix, identifier)

        self.buttons = buttons

    def matches(self, packet: Packet) -> bool:
        if not super().matches(packet):
            return False

        codes = [row['data'] for row in packet.data['rows']]
        for code in codes:
            if code in self.buttons.keys():
                return True
        return False

    def process(self, packet: Packet) -> None:
        codes = [row['data'] for row in packet.data['rows']]
        for code in codes:
            button = self.buttons.get(code, None)
            if button is not None:
                topic = f'{self.topic_prefix}/{button}'
                mqttc.publish(topic, 'pressed', qos=MQTT_QOS, retain=False)


class Receiver:
    def __init__(self, name: str, arguments: str):
        self.name = name
        self.arguments = arguments

        self.process: Optional[subprocess.Popen] = None

    def start(self):
        command = f'rtl_433 {self.arguments}'

        if '-F json' not in command:
            command += ' -F json'

        if '-C si' not in command:
            command += ' -C si'

        for custom_decoder in custom_decoders:
            command += f' -X {custom_decoder}'

        command_args = [arg.strip() for arg in command.split(' ') if arg.strip() != '']

        print(f'Running rtl_433[{self.name}] with arguments {" ".join(command_args[1:])}')
        self.process = subprocess.Popen(command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        threading.Thread(target=self.read_stderr_worker).start()
        threading.Thread(target=self.read_stdout_worker).start()

    def read_stderr_worker(self):
        while True:
            line = self.process.stderr.readline()
            if not line:
                break

            print(f'rtl_433[{self.name}]: {line.strip()}')

    def read_stdout_worker(self):
        print(f'rtl_433[{self.name}] is now reading packets.')
        received_first = False

        while True:
            line = self.process.stdout.readline()
            if not line:
                break

            packet = parse_rtl_433_packet(line)
            if packet is None:
                print(f'Error while parsing packet on receiver rtl_433[{self.name}]: {line.strip()}')
                continue

            if not received_first:
                received_first = True
                print(f'rtl_433[{self.name}] successfully received its first packet.')

            packet_receive_queue.put(packet)

        print(f'rtl_433[{self.name}] exited.')


mqttc: Optional[mqtt.Client] = None

custom_decoders: list[str] = []

receivers: list[Receiver] = []

sensors: list[RadioSensor] = []
ignored_sensors: list[SensorIdentifier] = []

packet_receive_queue: Queue[Packet] = Queue()


def parse_rtl_433_packet(line: str) -> Optional[Packet]:
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        return None

    # Fetch time from packet
    receive_time = datetime.strptime(data['time'], "%Y-%m-%d %H:%M:%S")
    del data['time']

    # Remove unnecessary keys
    for key in IGNORE_DATA_KEYS:
        if key in data:
            del data[key]

    return Packet(data, receive_time)


def process_packet(packet: Packet):
    sensor = find_sensor(packet)

    if packet.data.get('button', 0) == 1:
        print(f'Button pressed on {"unknown" if sensor is None else "known"} sensor: {json.dumps(packet.data)}')
        discord_message = f'**Button pressed {"unknown" if sensor is None else "known"} on sensor** :bell:\n' + \
                          '```json\n' + \
                          f'{json.dumps(packet.data, indent=2)}\n' + \
                          '```'

        send_discord_message(discord_message)
    elif sensor is None:
        print(f'Received packet from unknown sensor: {json.dumps(packet.data)}')

        discord_message = '**Received 433 MHz data from unknown sensor/device** :open_mouth:\n' + \
                          '```json\n' + \
                          f'{json.dumps(packet.data, indent=2)}\n' + \
                          '```'

        send_discord_message(discord_message)

    if sensor is not None:
        sensor.process(packet)


def send_discord_message(message: str):
    if DISCORD_WEBHOOK_URL is not None and DISCORD_WEBHOOK_URL != '':
        DiscordWebhook(url=DISCORD_WEBHOOK_URL, content=message).execute()


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


def process_packet_worker():
    previous_packets = PacketTimeRingBuffer(max_age=5)

    while True:
        packet = packet_receive_queue.get()

        if is_ignored(packet) or previous_packets.contains_duplicate(packet):
            continue

        previous_packets.add(packet)
        process_packet(packet)


def build_sensor(config: dict):
    sensor_type = config.get('type', 'temperature')
    if sensor_type == 'temperature':
        return TemperatureRadioSensor(
            topic_prefix=config['topic_prefix'],
            identifier=SensorIdentifier(config['identifier']),
        )
    elif sensor_type == 'button':
        return ButtonRadioSensor(
            topic_prefix=config['topic_prefix'],
            identifier=SensorIdentifier(config['identifier']),
            buttons=config['buttons'],
        )
    else:
        raise Exception(f'Unknown sensor type \'{sensor_type}\'')


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

    for receiver in config['receivers']:
        receivers.append(Receiver(
            name=receiver['name'],
            arguments=receiver['arguments'],
        ))


def main():
    global mqttc

    print(f'433-mqtt-bridge version {os.getenv("IMAGE_VERSION")}')

    print(f'{RECEIVERS_CONFIG_PATH=}')
    print(f'{SENSORS_CONFIG_PATH=}')
    print(f'{MQTT_BROKER_ADDRESS=}')
    print(f'{MQTT_BROKER_PORT=}')
    print(f'{MQTT_QOS=}')
    print(f'{MQTT_RETAIN=}')
    print(f'{DISCORD_WEBHOOK_URL=}')

    if LEGACY_RTL_433_ARGS is not None:
        print(f'Legacy RTL_433_ARGS argument found, creating receiver with arguments \'{LEGACY_RTL_433_ARGS}\' and name \'env\'.')
        receivers.append(Receiver(
            name='env',
            arguments=LEGACY_RTL_433_ARGS,
        ))

    load_receivers_config()
    load_sensors_config()

    print(f'Loaded {len(receivers)} receivers, {len(sensors)} sensors and {len(ignored_sensors)} ignored sensors.')

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.connect(MQTT_BROKER_ADDRESS, MQTT_BROKER_PORT, 60)
    mqttc.loop_start()

    for receiver in receivers:
        receiver.start()

    threading.Thread(target=process_packet_worker).start()


if __name__ == '__main__':
    main()
