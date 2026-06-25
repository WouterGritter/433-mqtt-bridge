import json
from queue import Empty

from . import events
from . import monitor
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


def process_packet(packet: Packet, duplicate: bool = False):
    sensor = registry.find_sensor(packet)
    ignored = sensor is None and registry.is_ignored(packet)

    # Receiver stats and the raw firehose count every packet the bridge sees — including
    # ignored ones and duplicates from a second receiver — so a receiver that only ever
    # loses the de-dup race still shows activity and the dashboard reflects reality.
    stats.record_receiver_packet(packet)
    events.emit('packet', {
        'receiver': packet.origin.name,
        'time': packet.receive_time.isoformat(),
        'data': packet.data,
        'sensor': sensor.topic_prefix if sensor is not None else None,
        'ignored': ignored,
        'duplicate': duplicate,
    })

    # Test sensors see every packet, independent of the known/ignored/unknown handling.
    process_test_sensors(packet)

    # Attribute the reception to this receiver for the per-sensor "seen by" view, whether
    # or not this copy is the one we go on to publish.
    if sensor is not None:
        stats.record_sensor_source(sensor.topic_prefix, packet.origin.name, packet.receive_time)

    if ignored:
        return

    if duplicate:
        # An identical packet was processed within the de-dup window (e.g. the same
        # reading picked up by another receiver, or an rtl_433 repeat). The reception was
        # attributed above; don't publish, persist, or re-notify. Push a light update so
        # the dashboard shows the extra receiver immediately.
        if sensor is not None:
            events.emit('sensor_source', {
                'sensor': sensor.topic_prefix,
                'snapshot': stats.sensor_snapshot(sensor.topic_prefix),
            })
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
    # Watch for two sensors colliding on the same random id (id-bearing sensors only);
    # guarded so monitoring can never break the publish pipeline.
    try:
        monitor.record(sensor, readings, packet.receive_time)
    except Exception as e:
        print(f'monitor: failed to evaluate {sensor.topic_prefix}: {e}')
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

    while not registry.shutdown_event.is_set():
        # Time out periodically so a shutdown is noticed even when no packets arrive.
        try:
            packet = registry.packet_receive_queue.get(timeout=0.5)
        except Empty:
            continue

        # Duplicates are still processed (to attribute the reception to their receiver),
        # but flagged so they don't get published/persisted/re-notified again.
        duplicate = previous_packets.contains_duplicate(packet)
        if not duplicate:
            previous_packets.add(packet)

        process_packet(packet, duplicate)
