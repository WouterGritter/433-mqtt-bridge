from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional

from .calculated_attributes import CalculatedAttributes, RainRateCalculatedAttribute
from .mqtt_client import publish
from .packet import Packet


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


class GenericRadioSensor(RadioSensor):
    def __init__(self, topic_prefix: str, identifier: SensorIdentifier, data_key_map: dict[str, str], calculated_attributes: Optional[list[CalculatedAttributes]] = None):
        super().__init__(topic_prefix, identifier)

        self.data_key_map = data_key_map
        self.calculated_attributes = calculated_attributes

    def process(self, packet: Packet) -> None:
        data = {mqtt_attribute: packet.data[data_key] for mqtt_attribute, data_key in self.data_key_map.items() if data_key in packet.data}
        for attribute, value in data.items():
            topic = f'{self.topic_prefix}/{attribute}'
            publish(topic, value)

        if self.calculated_attributes is not None:
            for attribute_calculator in self.calculated_attributes:
                additional_data = attribute_calculator.generate_calculated_attributes(data)
                if additional_data is not None:
                    for attribute, value in additional_data.items():
                        topic = f'{self.topic_prefix}/{attribute}'
                        publish(topic, value)


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


class TPMSRadioSensor(GenericRadioSensor):
    def __init__(self, topic_prefix: str, identifier: SensorIdentifier):
        super().__init__(
            topic_prefix,
            identifier,
            data_key_map={
                'pressure': 'pressure_kPa',
                'temperature': 'temperature_C',
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

        for code in packet.get_raw_data():
            if code in self.buttons.keys():
                return True
        return False

    def process(self, packet: Packet) -> None:
        for code in packet.get_raw_data():
            button = self.buttons.get(code, None)
            if button is not None:
                topic = f'{self.topic_prefix}/{button}'
                publish(topic, 'pressed', retain=False)


class LightningRadioSensor(RadioSensor):
    def __init__(self, topic: str, identifier: SensorIdentifier):
        super().__init__(topic, identifier)

        self.topic = topic
        self.last_strike_count: Optional[int] = None

    def process(self, packet: Packet) -> None:
        strike_count = int(packet.data['strike_count'])
        if self.last_strike_count is None:
            # First time we hear this sensor. Assume it isn't a lightning strike and record `strike_count`.
            self.last_strike_count = strike_count
            return

        if strike_count == self.last_strike_count:
            # Ignore when `strike_count` didn't change.
            return

        if strike_count == 0:
            # Ignore when `strike_count` is 0. This means the sensor got reset whilst we did receive a previously higher `strike_count`.
            self.last_strike_count = strike_count
            return

        storm_dist_km = int(packet.data['storm_dist_km'])
        publish(self.topic, str(storm_dist_km), retain=False)

        self.last_strike_count = strike_count


class DoorState(Enum):
    OPEN = ('open',)
    CLOSED = ('closed',)

    def __init__(self, mqtt_name: str):
        self.mqtt_name = mqtt_name


class DoorRadioSensor(RadioSensor):
    def __init__(self, topic: str, identifier: SensorIdentifier, door_open_code: str, door_closed_code: str, ignore_repeats: bool):
        super().__init__(topic, identifier)

        self.topic = topic
        self.door_open_code = door_open_code
        self.door_closed_code = door_closed_code
        self.ignore_repeats = ignore_repeats

        self.current_door_state: Optional[DoorState] = None

    def matches(self, packet: Packet) -> bool:
        if not super().matches(packet):
            return False

        raw_data = packet.get_raw_data()
        return self.door_open_code in raw_data or self.door_closed_code in raw_data

    def process(self, packet: Packet) -> None:
        raw_data = packet.get_raw_data()
        if self.door_open_code in raw_data:
            door_state = DoorState.OPEN
        elif self.door_closed_code in raw_data:
            door_state = DoorState.CLOSED
        else:
            return

        if self.ignore_repeats and door_state == self.current_door_state:
            return

        self.current_door_state = door_state
        publish(self.topic, door_state.mqtt_name)


def build_sensor(config: dict):
    sensor_type = config.get('type', 'temperature')
    if sensor_type == 'temperature':
        return TemperatureRadioSensor(
            topic_prefix=config['topic_prefix'],
            identifier=SensorIdentifier(config['identifier']),
        )
    elif sensor_type == 'tpms':
        return TPMSRadioSensor(
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
    elif sensor_type == 'door':
        return DoorRadioSensor(
            topic=config['topic'],
            identifier=SensorIdentifier(config['identifier']),
            door_open_code=config['door_open_code'],
            door_closed_code=config['door_closed_code'],
            ignore_repeats=config['ignore_repeats'],
        )
    elif sensor_type == 'lightning':
        return LightningRadioSensor(
            topic=config['topic'],
            identifier=SensorIdentifier(config['identifier']),
        )
    else:
        raise Exception(f'Unknown sensor type \'{sensor_type}\'')
