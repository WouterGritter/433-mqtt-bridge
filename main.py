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
    def __init__(self, data: dict[str, any], receive_time: datetime, origin: 'Receiver'):
        self.data = data
        self.receive_time = receive_time
        self.origin = origin

    def is_duplicate_data(self, other: Optional['Packet'], max_time_delta: Optional[float] = None) -> bool:
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
            if packet.is_duplicate_data(other):
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


class CalculatedAttributes(ABC):

    @abstractmethod
    def generate_calculated_attributes(self, received_attributes: dict[str, any]) -> Optional[dict[str, any]]:
        pass


class RainRateCalculatedAttribute(CalculatedAttributes):

    def __init__(self, time_window: float = 60 * 15):
        self.time_window = time_window
        self.rain_buffer: list[tuple[datetime, float]] = []

    def generate_calculated_attributes(self, received_attributes: dict[str, any]) -> Optional[dict[str, any]]:
        rain = received_attributes.get('rain')
        if rain is None:
            return None

        self.clean_buffer()
        self.rain_buffer.append((datetime.now(), rain))

        last_rain = self.rain_buffer[0][1]
        time_window_hr = self.time_window / 3600.0
        rain_delta = rain - last_rain
        rain_rate = rain_delta / time_window_hr

        return {
            'rain_rate': rain_rate,
        }

    def clean_buffer(self):
        now = datetime.now()
        for i in range(len(self.rain_buffer) - 1, -1, -1):
            age = (now - self.rain_buffer[i][0]).total_seconds()
            if age > self.time_window:
                del self.rain_buffer[i]


class RadioSensor(ABC):
    def __init__(self, topic_prefix: str, identifier: SensorIdentifier):
        self.topic_prefix = topic_prefix
        self.identifier = identifier

    def matches(self, packet: Packet) -> bool:
        return self.identifier.matches(packet)

    @abstractmethod
    def process(self, packet: Packet) -> None:
        pass


class GenericRadioSensor(RadioSensor):
    def __init__(self, topic_prefix: str, identifier: SensorIdentifier, data_key_map: dict[str, str], calculated_attributes: Optional[list[CalculatedAttributes]] = None):
        super().__init__(topic_prefix, identifier)

        self.data_key_map = data_key_map
        self.calculated_attributes = calculated_attributes

    def process(self, packet: Packet) -> None:
        data = {mqtt_attribute: packet.data[data_key] for mqtt_attribute, data_key in self.data_key_map.items() if data_key in packet.data}
        for attribute, value in data.items():
            topic = f'{self.topic_prefix}/{attribute}'
            mqttc.publish(topic, value, qos=MQTT_QOS, retain=MQTT_RETAIN)

        if self.calculated_attributes is not None:
            for attribute_calculator in self.calculated_attributes:
                additional_data = attribute_calculator.generate_calculated_attributes(data)
                if additional_data is not None:
                    for attribute, value in additional_data.items():
                        topic = f'{self.topic_prefix}/{attribute}'
                        mqttc.publish(topic, value, qos=MQTT_QOS, retain=MQTT_RETAIN)


class TemperatureRadioSensor(GenericRadioSensor):
    def __init__(self, topic_prefix: str, identifier: SensorIdentifier):
        super().__init__(
            topic_prefix,
            identifier,
            data_key_map={
                'temperature': 'temperature_C',
                'humidity': 'humidity',
            },
        )


class WeatherStationRadioSensor(GenericRadioSensor):
    def __init__(self, topic_prefix: str, identifier: SensorIdentifier):
        super().__init__(
            topic_prefix,
            identifier,
            data_key_map={
                'temperature': 'temperature_C',
                'humidity': 'humidity',
                'gustspeed': 'wind_max_m_s',
                'windspeed': 'wind_avg_m_s',
                'winddirection': 'wind_dir_deg',
                'rain': 'rain_mm',
                'light': 'light_lux',
                'uv': 'uv',
            },
            calculated_attributes=[
                RainRateCalculatedAttribute()
            ],
        )


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

            packet = parse_rtl_433_packet(line, self)
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


def parse_rtl_433_packet(line: str, receiver: Receiver) -> Optional[Packet]:
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

    return Packet(data, receive_time, receiver)


def process_packet(packet: Packet):
    sensor = find_sensor(packet)

    if packet.data.get('button', 0) == 1:
        print(f'Button pressed on {"unknown" if sensor is None else "known"} sensor on rtl_433[{packet.origin.name}]: {json.dumps(packet.data)}')
        discord_message = f'**Button pressed {"unknown" if sensor is None else "known"} on sensor on rtl_433[{packet.origin.name}]** :bell:\n' + \
                          f'```json\n' + \
                          f'{json.dumps(packet.data, indent=2)}\n' + \
                          f'```'

        send_discord_message(discord_message)
    elif sensor is None:
        print(f'Received packet from unknown sensor on rtl_433[{packet.origin.name}]: {json.dumps(packet.data)}')

        discord_message = f'**Received data from unknown sensor/device on rtl_433[{packet.origin.name}]** :open_mouth:\n' + \
                          f'```json\n' + \
                          f'{json.dumps(packet.data, indent=2)}\n' + \
                          f'```'

        send_discord_message(discord_message)

    if sensor is not None:
        sensor.process(packet)


def send_discord_message(message: str):
    if DISCORD_WEBHOOK_URL is not None and DISCORD_WEBHOOK_URL != '':
        try:
            DiscordWebhook(url=DISCORD_WEBHOOK_URL, content=message).execute()
        except Exception as e:
            print(f'An error occurred while trying to send a discord message: {e}')


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
        # print(f'rtl_433[{packet.origin.name}] received {json.dumps(packet.data)}')

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
    elif sensor_type == 'weather_station':
        return WeatherStationRadioSensor(
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
