import os
import subprocess
import json
import threading
from datetime import datetime
from typing import Optional
import yaml
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from discord_webhook import DiscordWebhook

load_dotenv()

SENSORS_CONFIG_PATH = os.getenv('SENSORS_CONFIG_PATH', 'sensors.yml')
MQTT_BROKER_ADDRESS = os.getenv('MQTT_BROKER_ADDRESS', 'localhost')
MQTT_BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))
MQTT_QOS = int(os.getenv('MQTT_QOS', '0'))
MQTT_RETAIN = os.getenv('MQTT_RETAIN', 'false') == 'true'
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')

RATE_LIMIT_RESET_INTERVAL = 60 * 60 * 8
RATE_LIMIT = 100

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


class RadioSensor:
    def __init__(self, topic_prefix: str, identifier: SensorIdentifier, keys: dict[str, str]):
        self.topic_prefix = topic_prefix
        self.identifier = identifier
        self.keys = keys

    def matches(self, packet: Packet) -> bool:
        return self.identifier.matches(packet)

    def extract(self, packet: Packet) -> dict[str, any]:
        data = packet.data
        return {key: data[value] for key, value in self.keys.items() if value in data}

    def process(self, packet: Packet) -> None:
        if not self.matches(packet):
            raise Exception('Packet does not match sensor')

        data = self.extract(packet)
        for attribute, value in data.items():
            topic = f'{self.topic_prefix}/{attribute}'
            mqttc.publish(topic, value, qos=MQTT_QOS, retain=MQTT_RETAIN)


mqttc: Optional[mqtt.Client] = None

sensors: list[RadioSensor] = []

ignored_sensors: list[SensorIdentifier] = []


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
    if sensor is not None:
        sensor.process(packet)
    else:
        print(f'Received packet from unknown sensor: {json.dumps(packet.data)}')

        discord_message = '**Received 433 MHz data from unknown sensor/device** :open_mouth:\n' + \
                          '```json\n' + \
                          f'{json.dumps(packet.data, indent=2)}\n' + \
                          '```'

        send_discord_message(discord_message)


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


def read_stderr(process):
    while True:
        line = process.stderr.readline()
        if not line:
            break

        print(f'rtl_433: {line.strip()}')


def read_stdout(process):
    previous_packets = PacketTimeRingBuffer(max_age=5)

    while True:
        line = process.stdout.readline()
        if not line:
            break

        packet = parse_rtl_433_packet(line)
        if packet is None:
            print('Error while parsing packet: ' + line.strip())
            continue

        if is_ignored(packet):
            print('Skipping ignored packet: ' + json.dumps(packet.data))
            continue

        if previous_packets.contains_duplicate(packet):
            print('Skipping duplicate packet: ' + json.dumps(packet.data))
            continue

        previous_packets.add(packet)

        process_packet(packet)


def main():
    global sensors, ignored_sensors, mqttc

    print(f'433-mqtt-bridge version {os.getenv("IMAGE_VERSION")}')

    print(f'{SENSORS_CONFIG_PATH=}')
    print(f'{MQTT_BROKER_ADDRESS=}')
    print(f'{MQTT_BROKER_PORT=}')
    print(f'{MQTT_QOS=}')
    print(f'{MQTT_RETAIN=}')
    print(f'{DISCORD_WEBHOOK_URL=}')

    with open(SENSORS_CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)

    sensors = [
        RadioSensor(
            topic_prefix=sensor['topic_prefix'],
            identifier=SensorIdentifier(sensor['identifier']),
            keys=sensor['keys'],
        )
        for sensor in config['sensors']
    ]

    ignored_sensors = [
        SensorIdentifier(sensor) for sensor in config['ignored_sensors']
    ]

    print(f'Loaded {len(sensors)} sensors and {len(ignored_sensors)} ignored sensors.')

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.connect(MQTT_BROKER_ADDRESS, MQTT_BROKER_PORT, 60)
    mqttc.loop_start()

    command = ['rtl_433', '-d', '1', '-t', 'digital_agc', '-F', 'json']

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    threading.Thread(target=read_stderr, args=(process,)).start()
    threading.Thread(target=read_stdout, args=(process,)).start()


if __name__ == '__main__':
    main()
