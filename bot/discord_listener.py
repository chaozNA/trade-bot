import logging
import discord
from typing import Callable, Awaitable

class DiscordListener:
    """Handles Discord message listening and routing to message handlers."""
    
    def __init__(self, token: str, channel_ids: set[str], message_handler: Callable[[discord.Message], Awaitable[None]]):
        self.token = token
        self.channel_ids = channel_ids
        self.message_handler = message_handler
        self.client = discord.Client()
        self._setup_events()
    
    def _setup_events(self):
        @self.client.event
        async def on_ready():
            logging.info(f"✅ Logged in as {self.client.user}")
        
        @self.client.event
        async def on_message(message: discord.Message):
            if str(message.channel.id) in self.channel_ids:
                logging.info(f"📩 Message from #{message.channel.name}: {message.content}")
                await self.message_handler(message)
        
        @self.client.event
        async def on_error(event, *args, **kwargs):
            logging.exception(f"❌ Error in event '{event}'")

    def run(self):
        try:
            self.client.run(self.token)
        except Exception as e:
            logging.error(f"Failed to run Discord client: {e}")
            raise
