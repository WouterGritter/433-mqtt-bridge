from typing import Any, Optional

import paho.mqtt.client as mqtt

from .config import MQTT_BROKER_ADDRESS, MQTT_BROKER_PORT, MQTT_QOS, MQTT_RETAIN

mqttc: Optional[mqtt.Client] = None

# Number of messages published since startup, surfaced on the dashboard.
published_count = 0


def connect():
    global mqttc

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.connect(MQTT_BROKER_ADDRESS, MQTT_BROKER_PORT, 60)
    mqttc.loop_start()


def publish(topic: str, value, retain: bool = MQTT_RETAIN):
    global published_count
    mqttc.publish(topic, value, qos=MQTT_QOS, retain=retain)
    published_count += 1


def status() -> dict[str, Any]:
    return {
        'connected': mqttc.is_connected() if mqttc is not None else False,
        'broker': f'{MQTT_BROKER_ADDRESS}:{MQTT_BROKER_PORT}',
        'published': published_count,
    }
