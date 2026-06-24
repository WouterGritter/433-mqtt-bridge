import json

from . import events
from . import registry
from . import stats
from . import storage
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


def process_test_sensors(packet: Packet):
    """Run a packet through any active test sensors and emit their parsed output. Test
    sensors are matched against every packet (known, unknown, or ignored) so a candidate
    config can be tried against live traffic, but their readings are never published or
    persisted."""
    for entry in registry.active_test_sensors():
        sensor = entry['sensor']
        if not sensor.matches(packet):
            continue
        readings = sensor.process(packet)
        events.emit('test', {
            'id': entry['id'],
            'receiver': packet.origin.name,
            'time': packet.receive_time.isoformat(),
            'raw': packet.data,
            'readings': {reading.topic: reading.value for reading in readings},
        })


def process_packet(packet: Packet):
    sensor = registry.find_sensor(packet)
    ignored = sensor is None and registry.is_ignored(packet)

    # Receiver stats and the raw firehose count every packet the bridge sees, including
    # ignored ones (they are still received), so the dashboard reflects reality.
    stats.record_receiver_packet(packet)
    events.emit('packet', {
        'receiver': packet.origin.name,
        'time': packet.receive_time.isoformat(),
        'data': packet.data,
        'sensor': sensor.topic_prefix if sensor is not None else None,
        'ignored': ignored,
    })

    # Test sensors see every packet, independent of the known/ignored/unknown handling.
    process_test_sensors(packet)

    if ignored:
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

    if sensor is None:
        registry.record_unknown(packet)
        return

    sensor.last_seen = packet.receive_time
    readings = sensor.process(packet)

    ts = packet.receive_time.timestamp()
    for reading in readings:
        publish(reading.topic, reading.value, retain=reading.retain)
        storage.record(sensor.topic_prefix, reading.topic, reading.value, ts)

    stats.record_sensor_packet(sensor, packet, readings)
    events.emit('reading', {
        'sensor': sensor.topic_prefix,
        'receiver': packet.origin.name,
        'time': packet.receive_time.isoformat(),
        'raw': packet.data,
        'readings': {reading.topic: reading.value for reading in readings},
        'snapshot': stats.sensor_snapshot(sensor.topic_prefix),
    })


def process_packet_worker():
    previous_packets = PacketTimeRingBuffer(max_age=IGNORE_DUPLICATE_PACKETS_TIMEFRAME)

    while True:
        packet = registry.packet_receive_queue.get()

        if previous_packets.contains_duplicate(packet):
            continue

        previous_packets.add(packet)
        process_packet(packet)
