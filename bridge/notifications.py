import base64
import json

from .config import BASE_URL, DISCORD_WEBHOOK_URL
from discord_webhook import DiscordWebhook


def send_discord_message(message: str):
    if DISCORD_WEBHOOK_URL is not None and DISCORD_WEBHOOK_URL != '':
        try:
            DiscordWebhook(url=DISCORD_WEBHOOK_URL, content=message).execute()
        except Exception as e:
            print(f'An error occurred while trying to send a discord message: {e}')


def encode_packet(packet_data: dict[str, any]) -> str:
    return base64.urlsafe_b64encode(json.dumps(packet_data).encode()).decode()


def build_claim_url(packet_data: dict[str, any]) -> str:
    return f'{BASE_URL}/claim?packet={encode_packet(packet_data)}'
