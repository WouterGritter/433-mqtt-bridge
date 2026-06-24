import json

from . import registry
from .config import IGNORE_DUPLICATE_PACKETS_TIMEFRAME
from .notifications import send_discord_message
from .packet import Packet, PacketTimeRingBuffer


def process_packet(packet: Packet):
    sensor = registry.find_sensor(packet)

    if sensor is None and registry.is_ignored(packet):
        return

    if packet.data.get('button', 0) == 1:
        print(f'Button pressed on {"unknown" if sensor is None else "known"} sensor on rtl_433[{packet.origin.name}]: {json.dumps(packet.data)}')

        discord_message = f'**Button pressed on {"unknown" if sensor is None else "known"} sensor on rtl_433[{packet.origin.name}]** :bell:\n' + \
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


def process_packet_worker():
    previous_packets = PacketTimeRingBuffer(max_age=IGNORE_DUPLICATE_PACKETS_TIMEFRAME)

    while True:
        packet = registry.packet_receive_queue.get()

        if previous_packets.contains_duplicate(packet):
            continue

        previous_packets.add(packet)
        process_packet(packet)
