import os
import logging
from dotenv import load_dotenv
import discord
from typing import Callable, Awaitable

load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_IDS = set(os.getenv("DISCORD_CHANNEL_IDS", "").split(","))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class DiscordListener:
    """Handles Discord message listening and routing to message handlers."""
    
    def __init__(self, message_handler: Callable[[discord.Message], Awaitable[None]]):
        """
        Initialize the Discord listener.
        
        Args:
            message_handler: Async function that will be called with each message
        """
        self.client = discord.Client(self_bot=True)  # Initialize as self-bot
        self.message_handler = message_handler
        self._setup_events()
    
    def _setup_events(self):
        """Set up Discord event handlers."""
        @self.client.event
        async def on_ready():
            logging.info(f"Logged in as {self.client.user}")
        
        @self.client.event
        async def on_message(message):
            if str(message.channel.id) in CHANNEL_IDS:
                logging.info(f"Received message from {message.channel.name}: {message.content}")
                await self.message_handler(message)
        
        @self.client.event
        async def on_error(event, *args, **kwargs):
            logging.exception(f"Error in event {event}")
    
    def run(self):
        """Start the Discord listener."""
        try:
            self.client.run(DISCORD_TOKEN)
        except Exception as e:
            logging.error(f"Error running Discord listener: {e}")
            raise