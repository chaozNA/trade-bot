import os
import logging
from dotenv import load_dotenv
from discord_listener import DiscordListener
import discord


def setup_environment():
    """Set up environment variables and logging"""
    # Load environment variables
    load_dotenv()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    # Validate required environment variables
    if not os.getenv("DISCORD_TOKEN"):
        raise ValueError("DISCORD_TOKEN environment variable is not set")
    
    if not os.getenv("DISCORD_CHANNEL_IDS"):
        raise ValueError("DISCORD_CHANNEL_IDS environment variable is not set")

async def message_handler(message: discord.Message):
    """Simple message handler that logs messages"""
    logging.info(f"Processing message: {message.content}")

def run_discord_listener():
    """Initialize and run the Discord listener"""
    listener = DiscordListener(message_handler)
    listener.run()

def main():
    """Main entry point"""
    try:
        setup_environment()
        logging.info("Starting Discord listener...")
        run_discord_listener()
    except Exception as e:
        logging.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    main()