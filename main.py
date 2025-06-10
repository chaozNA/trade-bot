import logging
import discord
from config import DISCORD_TOKEN, CHANNEL_IDS
from discord_listener import DiscordListener

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )


async def message_handler(message: discord.Message):
    """Handles Discord message, extracts trade signal."""
    logging.info(f"Processing message: {message.content}")
    # trade_signal = processor.process_message(message.content)
    # if trade_signal:
    #     logging.info(f"✅ Detected trade signal: {trade_signal}")
    #     # Placeholder: You could send it to Alpaca or queue for handling

def main():
    setup_logging()
    listener = DiscordListener(DISCORD_TOKEN, CHANNEL_IDS, message_handler)
    listener.run()

if __name__ == "__main__":
    main()
