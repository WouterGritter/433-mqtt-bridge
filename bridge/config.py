import os

from dotenv import load_dotenv

load_dotenv()

RECEIVERS_CONFIG_PATH = os.getenv('RECEIVERS_CONFIG_PATH', 'receivers.yml')
SENSORS_CONFIG_PATH = os.getenv('SENSORS_CONFIG_PATH', 'sensors.yml')
LEGACY_RTL_433_ARGS = os.getenv('RTL_433_ARGS')
IGNORE_DUPLICATE_PACKETS_TIMEFRAME = float(os.getenv('IGNORE_DUPLICATE_PACKETS_TIMEFRAME', '3'))
MQTT_BROKER_ADDRESS = os.getenv('MQTT_BROKER_ADDRESS', 'localhost')
MQTT_BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))
MQTT_QOS = int(os.getenv('MQTT_QOS', '0'))
MQTT_RETAIN = os.getenv('MQTT_RETAIN', 'false') == 'true'
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
BASE_URL = os.getenv('BASE_URL', '').rstrip('/')
WEB_HOST = os.getenv('WEB_HOST', '0.0.0.0')
WEB_PORT = int(os.getenv('WEB_PORT', '8000'))

IGNORE_DATA_KEYS = [
    'repeat',
]
