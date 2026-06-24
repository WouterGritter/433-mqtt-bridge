import os
import threading

from . import mqtt_client
from . import registry
from . import storage
from . import webapp
from .config import (
    BASE_URL,
    DISCORD_WEBHOOK_URL,
    IGNORE_DUPLICATE_PACKETS_TIMEFRAME,
    LEGACY_RTL_433_ARGS,
    MQTT_BROKER_ADDRESS,
    MQTT_BROKER_PORT,
    MQTT_QOS,
    MQTT_RETAIN,
    RECEIVERS_CONFIG_PATH,
    SENSORS_CONFIG_PATH,
    WEB_HOST,
    WEB_PORT,
)
from .processing import process_packet_worker
from .receiver import Receiver


def main():
    print(f'433-mqtt-bridge version {os.getenv("IMAGE_VERSION")}')

    print(f'{RECEIVERS_CONFIG_PATH=}')
    print(f'{SENSORS_CONFIG_PATH=}')
    print(f'{IGNORE_DUPLICATE_PACKETS_TIMEFRAME=}')
    print(f'{MQTT_BROKER_ADDRESS=}')
    print(f'{MQTT_BROKER_PORT=}')
    print(f'{MQTT_QOS=}')
    print(f'{MQTT_RETAIN=}')
    print(f'{DISCORD_WEBHOOK_URL=}')
    print(f'{BASE_URL=}')
    print(f'{WEB_HOST=}')
    print(f'{WEB_PORT=}')

    if LEGACY_RTL_433_ARGS is not None:
        print(f'Legacy RTL_433_ARGS argument found, creating receiver with arguments \'{LEGACY_RTL_433_ARGS}\' and name \'env\'.')
        registry.receivers.append(Receiver(
            name='env',
            arguments=LEGACY_RTL_433_ARGS,
        ))

    registry.load_receivers_config()
    registry.load_sensors_config()

    print(f'Loaded {len(registry.receivers)} receivers, {len(registry.sensors)} sensors and {len(registry.ignored_sensors)} ignored sensors.')

    storage.init()

    mqtt_client.connect()

    for receiver in registry.receivers:
        receiver.start()

    threading.Thread(target=process_packet_worker).start()

    # Runs in the foreground (main thread) for clean signal handling; the receiver and
    # packet-processing threads keep running alongside it.
    webapp.run()


if __name__ == '__main__':
    main()
