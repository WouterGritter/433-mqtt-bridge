# 433-mqtt-bridge

Bridges 433 MHz sensor data to MQTT. It runs one or more [`rtl_433`](https://github.com/merbanan/rtl_433)
receivers (locally or remotely over `rtl_tcp`), decodes/parses the packets, maps known
sensors to clean MQTT topics, and publishes their readings. Unknown or unconfigured
devices are reported to Discord so they can be identified and added.

## What it does

- Runs multiple `rtl_433` receivers in parallel — useful for covering multiple
  frequencies and/or multiple physical locations.
- Matches incoming packets against configured **sensors** and publishes their values
  to MQTT under a topic you choose.
- De-duplicates repeated packets within a short time window.
- Supports custom `rtl_433` decoders (e.g. for cheap buttons and door sensors).
- Reports packets from unknown devices (and ignores ones you don't care about) via a
  Discord webhook.

## Requirements

- Python 3.10+
- [`rtl_433`](https://github.com/merbanan/rtl_433) installed and on your `PATH`
- An RTL-SDR (or other supported SDR), and an MQTT broker
- Python packages (see `requirements.txt`)

```sh
pip install -r requirements.txt
```

## Configuration

The bridge is configured through a `.env` file and two YAML files. See `.env.example`,
`example.receivers.yml`, and `example.sensors.yml` for working starting points.

### Environment (`.env`)

| Variable | Default | Description |
|---|---|---|
| `MQTT_BROKER_ADDRESS` | `localhost` | MQTT broker hostname |
| `MQTT_BROKER_PORT` | `1883` | MQTT broker port |
| `MQTT_QOS` | `0` | QoS used when publishing |
| `MQTT_RETAIN` | `false` | Retain published messages |
| `DISCORD_WEBHOOK_URL` | _(none)_ | Webhook for unknown-device / status alerts |
| `RECEIVERS_CONFIG_PATH` | `receivers.yml` | Path to the receivers config |
| `SENSORS_CONFIG_PATH` | `sensors.yml` | Path to the sensors config |
| `IGNORE_DUPLICATE_PACKETS_TIMEFRAME` | `3` | Seconds within which identical packets are dropped |

### Receivers (`receivers.yml`)

Each receiver is an `rtl_433` invocation. `custom_decoders` are passed to every
receiver via `-X`. (`-F json` and `-C si` are added automatically.)

```yaml
receivers:
  - name: local-433
    arguments: '-d 1 -t digital_atc'

custom_decoders:
  - 'n=doorsensor,m=OOK_PWM,s=444,l=1168,r=1148,g=0,t=290,y=0,bits=25'
```

### Sensors (`sensors.yml`)

Each sensor has a `type`, a target MQTT `topic`/`topic_prefix`, and an `identifier`
(the set of `rtl_433` fields — e.g. `model`, `id`, `channel` — that must all match).
Supported types: `temperature`, `tpms`, `weather_station`, `button`, `door`,
`lightning`. `ignored_sensors` lists identifiers to silently drop.

```yaml
sensors:
  - type: temperature
    topic_prefix: 'outdoor/terras'
    identifier:
      model: 'Nexus-T'
      id: 251
      channel: 1

ignored_sensors:
  - model: 'Nexa-Security'
```

## Running

From the project root:

```sh
python3 -m bridge
```

### As a systemd service

A unit file is included (`433-mqtt-bridge.service`). Adjust `User` and
`WorkingDirectory` to your install location, then:

```sh
sudo cp 433-mqtt-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now 433-mqtt-bridge
```

The service runs `python3 -m bridge` from its `WorkingDirectory`, so keep the
`bridge/` package and your `.env` / `*.yml` files there.
