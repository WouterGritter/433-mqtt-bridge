import asyncio
import base64
import json
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from . import events
from . import registry
from .config import WEB_HOST, WEB_PORT


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Hand the event bus the loop the web server runs on, so the threaded packet
    # pipeline can push events to WebSocket clients via call_soon_threadsafe.
    events.set_loop(asyncio.get_running_loop())
    yield


app = FastAPI(title='433-mqtt-bridge', lifespan=lifespan)
templates = Jinja2Templates(directory=str(Path(__file__).parent / 'templates'))


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


def run():
    """Run the web server in the foreground (blocking)."""
    print(f'Starting web server on {WEB_HOST}:{WEB_PORT}')
    server = uvicorn.Server(uvicorn.Config(app, host=WEB_HOST, port=WEB_PORT, log_level='warning'))
    # Leave signal handling to Python's default so SIGTERM terminates the process
    # immediately (the receiver/processing threads are non-daemon and would otherwise
    # keep it alive). This matches the bridge's pre-web shutdown behaviour.
    server.install_signal_handlers = lambda: None
    server.run()
