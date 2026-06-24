from .config import DISCORD_WEBHOOK_URL
from discord_webhook import DiscordWebhook


def send_discord_message(message: str):
    if DISCORD_WEBHOOK_URL is not None and DISCORD_WEBHOOK_URL != '':
        try:
            DiscordWebhook(url=DISCORD_WEBHOOK_URL, content=message).execute()
        except Exception as e:
            print(f'An error occurred while trying to send a discord message: {e}')
