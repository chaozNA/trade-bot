import logging
from typing import Optional
import discord
from config.config import DISCORD_TOKEN, CHANNEL_IDS
from .discord_listener import DiscordListener
from .trade_manager import TradeManager
from .storage_manager import StorageManager

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

class TradeBotApp:
    def __init__(self):
        self.storage_manager = StorageManager()
        self.trade_manager = TradeManager(storage_manager=self.storage_manager)

    async def message_handler(self, message: discord.Message):
        """Handles Discord message, extracts trade signal."""
        root_trade_id = None
        if message.reference:
            root_trade_id = await self._find_root_trade_message(message, set(self.trade_manager.active_trades.keys()))

        self.trade_manager.process_message(
            message_content=message.content,
            message_id=message.id,
            parent_message_id=root_trade_id
        )

    async def _find_root_trade_message(self, message: discord.Message, active_trade_ids: set[int]) -> Optional[int]:
        """Recursively finds the root message of a trade conversation."""
        if not message.reference:
            return None

        parent_message_id = message.reference.message_id
        if parent_message_id in active_trade_ids:
            return parent_message_id

        try:
            parent_message = await message.channel.fetch_message(parent_message_id)
            return await self._find_root_trade_message(parent_message, active_trade_ids)
        except discord.NotFound:
            logging.warning(f"Could not find parent message {parent_message_id}")
            return None
        except discord.Forbidden:
            logging.error(f"Do not have permissions to fetch message {parent_message_id}")
            return None

    def run(self):
        logging.info(f"Channels: {CHANNEL_IDS}")
        listener = DiscordListener(DISCORD_TOKEN, CHANNEL_IDS, self.message_handler)
        listener.run()

if __name__ == "__main__":
    setup_logging()
    app = TradeBotApp()
    app.run()
