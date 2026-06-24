import json
from datetime import datetime
from typing import Optional, TYPE_CHECKING

from .config import IGNORE_DATA_KEYS

if TYPE_CHECKING:
    from .receiver import Receiver


class Packet:
    def __init__(self, data: dict[str, any], receive_time: datetime, origin: 'Receiver'):
        self.data = data
        self.receive_time = receive_time
        self.origin = origin

    def get_raw_data(self) -> Optional[list[str]]:
        if 'rows' not in self.data:
            return None

        return [row['data'] for row in self.data['rows']]

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


def parse_rtl_433_packet(line: str, receiver: 'Receiver') -> Optional[Packet]:
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
