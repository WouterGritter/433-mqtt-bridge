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
- Serves a small web interface to **re-claim** an unknown sensor as an existing one —
  handy for sensors whose `id` changes on every battery swap (see below).

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
| `BASE_URL` | _(none)_ | Public URL of the web interface (behind your reverse proxy); enables the "claim" links in Discord |
| `WEB_HOST` | `0.0.0.0` | Address the web interface binds to |
| `WEB_PORT` | `8000` | Port the web interface binds to |
| `RECEIVERS_CONFIG_PATH` | `receivers.yml` | Path to the receivers config |
| `SENSORS_CONFIG_PATH` | `sensors.yml` | Path to the sensors config |
| `IGNORE_DUPLICATE_PACKETS_TIMEFRAME` | `3` | Seconds within which identical packets are dropped |
| `STATS_DB_PATH` | `stats.db` | SQLite file storing reading history for the dashboard |
| `STATS_RETENTION_DAYS` | `30` | How long reading history is kept before pruning |

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

## Web interface & dashboard

The bridge serves a web interface (FastAPI) on `WEB_HOST:WEB_PORT`. It is
**unauthenticated** — run it behind your reverse proxy, not exposed directly.

### Dashboard (`/`)

A live dashboard (updated over a WebSocket) showing:

- **Sensors** — per-sensor cards with the latest parsed values, battery and signal
  status, packet rate, the average interval between recent messages, a freshness/stale
  indicator, and a sparkline of recent history (persisted to SQLite, so it survives
  restarts).
- **Receivers** — per-receiver status (running, restart count, packet rate, last seen,
  average signal) with a **Restart** button.
- **Raw feed** — a live firehose of every received packet, with a text filter, tagging
  each as known / unknown / ignored.
- **MQTT status** — connection state and a published-message counter.

From the dashboard you can also:

- **Add, edit and remove sensors** (and ignored devices) — changes are written to
  `sensors.yml` and applied to the running process immediately.
- **Test a sensor before saving it** — enter a candidate config and watch it match live
  traffic, showing the **raw JSON received alongside the parsed values** for the chosen
  type. The test sensor is never saved and **never published to MQTT**.
- **Claim or adopt unknown devices** — the most recent unknown readings are listed; for
  each you can re-claim it onto an existing sensor (see below) or create a new sensor
  prefilled from its packet (and test it first).
- **Manage custom decoders** — add/remove the `rtl_433` `-X` specs in `receivers.yml`
  (applied after a receiver restart).

### Claiming sensors

Sensors that include an `id` field get a **new `id` every time you swap their
batteries**, which would otherwise mean editing `sensors.yml` and restarting.

When an unknown device with an `id` is seen, the Discord alert includes a **"Claim this
sensor"** link (requires `BASE_URL`); the same packets also appear in the dashboard's
unknown list. Opening the claim view shows the unknown packet and a dropdown of
configured sensors whose identifier matches on everything *except* `id` (e.g. same
`model` and `channel`). Picking one updates that sensor's `id` — written to
`sensors.yml` **and** applied to the running process immediately, so no restart is
needed. Devices without an `id` (buttons, door sensors) use stable raw codes and are
unaffected.

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
