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
STATS_DB_PATH = os.getenv('STATS_DB_PATH', 'stats.db')
STATS_RETENTION_DAYS = float(os.getenv('STATS_RETENTION_DAYS', '30'))

# --- ping-pong / id-collision monitor (see monitor.py) ---------------------
# Detects an id-bearing sensor whose readings alternate between two value levels, the
# sign of two distinct sensors sharing the same random `id`.
MONITOR_ENABLED = os.getenv('MONITOR_ENABLED', 'true') == 'true'
# Sliding window of recent readings to judge. Its length combined with MIN_SAMPLES is
# also the frequency gate: a sensor must produce at least MIN_SAMPLES readings within
# this many seconds to be evaluated at all, which naturally excludes sensors that report
# too rarely to tell a real ping-pong from coincidental back-to-back values (flaky temp
# sensors, TPMS, lightning).
MONITOR_WINDOW_SECONDS = float(os.getenv('MONITOR_WINDOW_SECONDS', '900'))
MONITOR_MIN_SAMPLES = int(os.getenv('MONITOR_MIN_SAMPLES', '8'))
# Separation gate. The gap between the two value levels must exceed this in the reading's
# native units (kills tiny quantization dither, e.g. 20.0/20.1 °C) AND be at least this
# many times the spread within each level.
MONITOR_MIN_AMPLITUDE = float(os.getenv('MONITOR_MIN_AMPLITUDE', '1.0'))
MONITOR_SEPARATION_RATIO = float(os.getenv('MONITOR_SEPARATION_RATIO', '3.0'))
# Alternation gate: minimum Wald-Wolfowitz runs-test z-score. A single smoothly varying
# sensor yields a negative z; a ping-pong yields a large positive one.
MONITOR_RUNS_Z = float(os.getenv('MONITOR_RUNS_Z', '2.0'))
# Minimum seconds between repeat alerts for the same sensor (it needs a physical fix).
MONITOR_COOLDOWN_SECONDS = float(os.getenv('MONITOR_COOLDOWN_SECONDS', '21600'))

IGNORE_DATA_KEYS = [
    'repeat',
]
