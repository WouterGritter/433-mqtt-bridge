from typing import Optional

import paho.mqtt.client as mqtt

from .config import MQTT_BROKER_ADDRESS, MQTT_BROKER_PORT, MQTT_QOS, MQTT_RETAIN

mqttc: Optional[mqtt.Client] = None


def connect():
    global mqttc

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.connect(MQTT_BROKER_ADDRESS, MQTT_BROKER_PORT, 60)
    mqttc.loop_start()


def publish(topic: str, value, retain: bool = MQTT_RETAIN):
    mqttc.publish(topic, value, qos=MQTT_QOS, retain=retain)
