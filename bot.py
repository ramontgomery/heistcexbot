import os
import discord
import asyncio
import logging  # Import the logging module
from CryptoPollingMgr import CryptoPollingService
from discord import Intents



# Configure the logging module
logging.basicConfig(level=logging.INFO)  # Set the logging level to INFO

class DiscordBot:
    def __init__(self, token, polling_service, channel_id):
        # Define your intents
        intents = Intents.default()
        intents.typing = False
        intents.presences = False

        # Create a discord.Client() instance with the specified intents
        self.client = discord.Client(intents=intents)
        self.token = token
        self.polling_service = polling_service
        self.channel_id = channel_id

    async def start(self):
        # Register the event handlers
        self.client.event(self.on_ready)
        
        # Start the Discord client
        await self.client.start(self.token)

    async def on_ready(self):
        logging.info(f'{self.client.user} has connected to Discord!')

        # Start the crypto service monitoring task
        self.client.loop.create_task(self.polling_service.monitor_and_notify_satoshi_rate_changes())

        channel = self.client.get_channel(self.channel_id)
        if channel:
            while True:
                message = await self.polling_service.check_for_trigger()
                if message:
                    try:
                        await channel.send(message)
                        logging.info(f"Sent message: {message}")
                    except Exception as e:
                        logging.error(f"Error sending message: {e}")
                else:
                    logging.info("No new messages to send.")
                await asyncio.sleep(120)

# Initialize the crypto polling service
crypto_service = CryptoPollingService()

# Initialize the Discord bot
bot_token = os.environ.get('DISCORD_BOT_TOKEN')
channel_id = '1184868864148910152'  # Replace with your channel ID

discord_bot = DiscordBot(bot_token, crypto_service, channel_id)

# Start the Discord bot
asyncio.run(discord_bot.start())
