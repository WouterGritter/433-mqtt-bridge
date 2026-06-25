import asyncio
import base64
import json
import signal
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import uvicorn
from fastapi import Body, FastAPI, Form, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import events
from . import monitor
from . import mqtt_client
from . import registry
from . import stats
from . import storage
from .config import WEB_HOST, WEB_PORT
from .notifications import encode_packet


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Hand the event bus the loop the web server runs on, so the threaded packet
    # pipeline can push events to WebSocket clients via call_soon_threadsafe.
    events.set_loop(asyncio.get_running_loop())
    yield
    # Graceful shutdown: stop the background workers and the MQTT client. The signal
    # handler in run() has usually already set the event and terminated the receivers;
    # repeating it here is harmless and also covers a programmatic server stop.
    print('Shutting down…')
    registry.shutdown_event.set()
    for receiver in registry.receivers:
        receiver.stop()
    for thread in registry.background_threads:
        thread.join(timeout=5)
    mqtt_client.disconnect()
    print('Shutdown complete.')


app = FastAPI(title='433-mqtt-bridge', lifespan=lifespan)
templates = Jinja2Templates(directory=str(Path(__file__).parent / 'templates'))
app.mount('/static', StaticFiles(directory=str(Path(__file__).parent / 'static')), name='static')


def _decode_packet(encoded: str) -> dict[str, any]:
    encoded += '=' * (-len(encoded) % 4)  # restore any stripped base64 padding
    return json.loads(base64.urlsafe_b64decode(encoded.encode()))


def _format_identifier(identifier: dict[str, any]) -> str:
    return ', '.join(f'{key}={value}' for key, value in identifier.items())


def _seconds_since_seen(sensor) -> float:
    """Seconds since the sensor last matched a packet; +inf if never seen this run."""
    if sensor.last_seen is None:
        return float('inf')
    return (datetime.now() - sensor.last_seen).total_seconds()


def _format_age(seconds: float) -> str:
    if seconds == float('inf'):
        return 'not seen since restart'
    seconds = int(seconds)
    if seconds < 60:
        return f'{seconds}s ago'
    if seconds < 3600:
        return f'{seconds // 60}m ago'
    if seconds < 86400:
        return f'{seconds // 3600}h ago'
    return f'{seconds // 86400}d ago'


def _message(request: Request, title: str, message: str) -> HTMLResponse:
    return templates.TemplateResponse(request, 'message.html', {'title': title, 'message': message})


@app.get('/claim', response_class=HTMLResponse)
def claim_get(request: Request, packet: str):
    try:
        packet_data = _decode_packet(packet)
    except Exception:
        return _message(request, 'Invalid link', 'Could not decode the sensor data in this link.')

    if 'id' not in packet_data:
        return _message(request, 'Cannot claim', 'This packet has no id field, so there is nothing to claim.')

    # Stalest first: the sensor being re-claimed is usually the one that went quiet
    # (its old id stopped reporting after the battery swap).
    matches = sorted(registry.find_claim_candidates(packet_data), key=lambda m: _seconds_since_seen(m[1]), reverse=True)
    candidates = [
        {
            'index': index,
            'label': f'{sensor.topic_prefix} - {_format_identifier(sensor.identifier.identifier)}',
            'last_seen': _format_age(_seconds_since_seen(sensor)),
        }
        for index, sensor in matches
    ]

    return templates.TemplateResponse(request, 'claim.html', {
        'title': 'Claim sensor',
        'packet': packet,
        'packet_json': json.dumps(packet_data, indent=2),
        'new_id': packet_data['id'],
        'candidates': candidates,
    })


@app.post('/claim', response_class=HTMLResponse)
def claim_post(request: Request, packet: str = Form(...), sensor_index: int = Form(...)):
    try:
        packet_data = _decode_packet(packet)
    except Exception:
        return _message(request, 'Invalid request', 'Could not decode the sensor data.')

    if 'id' not in packet_data:
        return _message(request, 'Cannot claim', 'This packet has no id field.')

    new_id = packet_data['id']
    candidate_indices = {index for index, _ in registry.find_claim_candidates(packet_data)}
    if sensor_index not in candidate_indices:
        return _message(request, 'Sensor no longer matches', 'The selected sensor is no longer a valid match. Please reopen the link and try again.')

    sensor = registry.sensors[sensor_index]
    old_id = sensor.identifier.identifier.get('id')
    registry.claim_sensor(sensor_index, new_id)

    return templates.TemplateResponse(request, 'claimed.html', {
        'title': 'Sensor claimed',
        'topic': sensor.topic_prefix,
        'old_id': old_id,
        'new_id': new_id,
    })


# --- Dashboard ------------------------------------------------------------

def _sensor_list() -> list[dict[str, any]]:
    """Configured sensors paired with their raw config (for the edit form) and live stats
    (None until first seen). `index` is the position in sensors.yml, used by the CRUD
    endpoints."""
    with registry.lock:
        configs = registry.get_sensor_configs()
        return [
            {
                'index': index,
                'key': sensor.topic_prefix,
                'type': sensor.type_name,
                'identifier': dict(sensor.identifier.identifier),
                'config': configs[index] if index < len(configs) else None,
                'stats': stats.sensor_snapshot(sensor.topic_prefix),
            }
            for index, sensor in enumerate(registry.sensors)
        ]


def _receiver_list() -> list[dict[str, any]]:
    """Configured receivers paired with their live stats."""
    snapshots = stats.all_receiver_snapshots()
    return [
        {'name': receiver.name, 'arguments': receiver.arguments, **snapshots.get(receiver.name, {})}
        for receiver in registry.receivers
    ]


@app.get('/', response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse(request, 'dashboard.html', {'title': 'Dashboard'})


@app.get('/api/sensors')
def api_sensors():
    return _sensor_list()


@app.get('/api/receivers')
def api_receivers():
    return _receiver_list()


@app.get('/api/unknowns')
def api_unknowns():
    # Include the encoded packet so the dashboard can link straight into the claim flow.
    unknowns = registry.list_recent_unknowns()
    for entry in unknowns:
        entry['encoded'] = encode_packet(entry['data'])
    return unknowns


@app.get('/api/status')
def api_status():
    return {'mqtt': mqtt_client.status()}


@app.get('/api/sensors/history')
def api_sensor_history(key: str, since: float = 0.0):
    """Reading history for one sensor since a unix timestamp, for the sparklines."""
    return storage.query_history(key, since)


@app.post('/api/receivers/{name}/restart')
def api_restart_receiver(name: str):
    for receiver in registry.receivers:
        if receiver.name == name:
            return {'restarted': receiver.restart()}
    return JSONResponse({'error': 'No such receiver'}, status_code=404)


# --- sensor CRUD ----------------------------------------------------------

def _crud(action):
    """Run a registry mutation, mapping its errors to clean HTTP responses: invalid input
    (e.g. a bad sensor config raised by build_sensor) becomes 400, a bad index 404."""
    try:
        return action()
    except IndexError as e:
        return JSONResponse({'error': str(e)}, status_code=404)
    except KeyError as e:
        return JSONResponse({'error': f'Missing required field: {e}'}, status_code=400)
    except Exception as e:  # ValueError / build_sensor errors / etc.
        return JSONResponse({'error': str(e)}, status_code=400)


@app.post('/api/sensors')
def api_add_sensor(config: dict = Body(...)):
    return _crud(lambda: {'key': registry.add_sensor(config)})


@app.put('/api/sensors/{index}')
def api_update_sensor(index: int, config: dict = Body(...)):
    def action():
        old_key, new_key = registry.update_sensor(index, config)
        if old_key != new_key:
            stats.drop_sensor(old_key)
            monitor.drop_sensor(old_key)
        return {'key': new_key}
    return _crud(action)


@app.delete('/api/sensors/{index}')
def api_delete_sensor(index: int):
    def action():
        key = registry.remove_sensor(index)
        stats.drop_sensor(key)
        monitor.drop_sensor(key)
        return {'removed': key}
    return _crud(action)


@app.get('/api/ignored')
def api_ignored():
    return registry.list_ignored_sensors()


@app.post('/api/ignored')
def api_add_ignored(identifier: dict = Body(...)):
    return _crud(lambda: registry.add_ignored_sensor(identifier) or {'ok': True})


@app.delete('/api/ignored/{index}')
def api_delete_ignored(index: int):
    return _crud(lambda: registry.remove_ignored_sensor(index) or {'ok': True})


# --- test sensors ---------------------------------------------------------

@app.post('/api/test-sensors')
def api_add_test_sensor(config: dict = Body(...)):
    return _crud(lambda: registry.add_test_sensor(config))


@app.post('/api/test-sensors/{test_id}/renew')
def api_renew_test_sensor(test_id: str):
    return {'ok': registry.renew_test_sensor(test_id)}


@app.delete('/api/test-sensors/{test_id}')
def api_delete_test_sensor(test_id: str):
    registry.remove_test_sensor(test_id)
    return {'ok': True}


# --- custom decoders ------------------------------------------------------

@app.get('/api/decoders')
def api_decoders():
    return registry.list_custom_decoders()


@app.post('/api/decoders')
def api_add_decoder(payload: dict = Body(...)):
    return _crud(lambda: registry.add_custom_decoder(payload.get('decoder', '')) or {'ok': True})


@app.delete('/api/decoders/{index}')
def api_delete_decoder(index: int):
    return _crud(lambda: registry.remove_custom_decoder(index) or {'ok': True})


@app.websocket('/ws')
async def ws(websocket: WebSocket):
    """Stream live events (packet/reading/unknown/receiver_status) to the dashboard."""
    await websocket.accept()
    queue = events.subscribe()

    # Reading from the socket lets us notice a client disconnect promptly even while the
    # event stream is idle (we otherwise only ever write).
    async def watch_disconnect():
        try:
            while True:
                await websocket.receive_text()
        except Exception:
            pass

    watcher = asyncio.create_task(watch_disconnect())
    try:
        while True:
            event = await queue.get()
            await websocket.send_json(event)
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        watcher.cancel()
        events.unsubscribe(queue)


def run():
    """Run the web server in the foreground (blocking) until SIGINT/SIGTERM."""
    print(f'Starting web server on {WEB_HOST}:{WEB_PORT}')
    server = uvicorn.Server(uvicorn.Config(
        app, host=WEB_HOST, port=WEB_PORT, log_level='warning', timeout_graceful_shutdown=5,
    ))

    # uvicorn captures SIGINT/SIGTERM itself: the first signal triggers a graceful stop
    # (which runs our lifespan shutdown — terminating receivers, joining workers,
    # disconnecting MQTT), a second forces an immediate exit. When it finishes it re-raises
    # the signal to whatever handler was installed beforehand; install no-op handlers so
    # that re-raise is silent instead of turning SIGINT into a KeyboardInterrupt traceback.
    signal.signal(signal.SIGINT, lambda *_: None)
    signal.signal(signal.SIGTERM, lambda *_: None)

    server.run()
