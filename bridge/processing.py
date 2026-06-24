import json

from . import registry
from .config import BASE_URL, IGNORE_DUPLICATE_PACKETS_TIMEFRAME
from .mqtt_client import publish
from .notifications import build_claim_url, send_discord_message
from .packet import Packet, PacketTimeRingBuffer


def with_claim_link(message: str, packet: Packet) -> str:
    """Append a 'claim' link to a Discord message when the packet carries an `id`.
    Sensors that carry an `id` get a new one on every battery swap, so this offers a
    way to re-claim the packet as an existing configured sensor (buttons/doors that only
    send raw codes have no `id` and are left untouched)."""
    if BASE_URL and 'id' in packet.data:
        return message + f'\n:link: [Claim this sensor]({build_claim_url(packet.data)})'
    return message


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

        send_discord_message(with_claim_link(discord_message, packet))
    elif sensor is None:
        print(f'Received packet from unknown sensor on rtl_433[{packet.origin.name}]: {json.dumps(packet.data)}')

        discord_message = f'**Received data from unknown sensor/device on rtl_433[{packet.origin.name}]** :open_mouth:\n' + \
                          f'```json\n' + \
                          f'{json.dumps(packet.data, indent=2)}\n' + \
                          f'```'

        send_discord_message(with_claim_link(discord_message, packet))

    if sensor is not None:
        sensor.last_seen = packet.receive_time
        for reading in sensor.process(packet):
            publish(reading.topic, reading.value, retain=reading.retain)


def process_packet_worker():
    previous_packets = PacketTimeRingBuffer(max_age=IGNORE_DUPLICATE_PACKETS_TIMEFRAME)

    while True:
        packet = registry.packet_receive_queue.get()

        if previous_packets.contains_duplicate(packet):
            continue

        previous_packets.add(packet)
        process_packet(packet)
