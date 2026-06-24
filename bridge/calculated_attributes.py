from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional


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
