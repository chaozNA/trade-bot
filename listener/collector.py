import asyncio
import discord
from datetime import datetime
import json
import os
import time
from typing import Set, Optional
from dotenv import load_dotenv
from commons.redis.redis_client import redis_client
from utils.logger import get_logger
from commons.mongodb.mongodb_client import MongoDBClient
from discord.errors import HTTPException

load_dotenv(override=True)
logger = get_logger('collector')

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_IDS = {cid for cid in os.getenv("DISCORD_CHANNEL_IDS", "").split(",") if cid}
logger.info(f"Channels: {CHANNEL_IDS}")
QUEUE_NAME = os.getenv("MESSAGE_QUEUE_NAME", "message_queue")
MESSAGES_COLLECTION = os.getenv("MESSAGES_COLLECTION", "raw_messages")

if not DISCORD_TOKEN:
    logger.error("DISCORD_TOKEN environment variable is not set")
    raise ValueError("DISCORD_TOKEN missing.")
if not CHANNEL_IDS:
    logger.error("DISCORD_CHANNEL_IDS environment variable is empty or invalid")
    raise ValueError("DISCORD_CHANNEL_IDS missing.")
if not QUEUE_NAME:
    logger.error("MESSAGE_QUEUE_NAME environment variable is not set")
    raise ValueError("MESSAGE_QUEUE_NAME missing.")
if not MESSAGES_COLLECTION:
    logger.error("MESSAGES_COLLECTION environment variable is not set")
    raise ValueError("MESSAGES_COLLECTION missing.")
if not os.getenv("ATLAS_CONNECTION"):
    logger.error("ATLAS_CONNECTION environment variable is not set")
    raise ValueError("ATLAS_CONNECTION missing.")

logger.info(f"Starting message collector for channels: {', '.join(CHANNEL_IDS)}")

def sanitize_for_log(s: str) -> str:
    """Remove non-ASCII characters to prevent encoding errors in logging."""
    return s.encode('ascii', 'ignore').decode('ascii')

class MessageCollector(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        logger.debug("Initializing MessageCollector")
        try:
            self.mongo_client = MongoDBClient()
            self.messages_collection = self.mongo_client.get_collection(MESSAGES_COLLECTION)
            self.closed = False
            logger.debug("MongoDB client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB client: {e}")
            raise

    async def on_ready(self):
        logger.info(f"Logged in as {self.user} (ID: {self.user.id})")
        logger.info(f"Monitoring {len(CHANNEL_IDS)} channel(s)")
        try:
            self.mongo_client.db.command("ping")
            redis_client.redis.ping()
            logger.info("MongoDB and Redis connections verified")
        except Exception as e:
            logger.error(f"Connection health check failed: {e}")
            await self.close()

    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
            
        if str(message.channel.id) not in CHANNEL_IDS:
            return
        
        start_time = time.time()
        parent_id = message.reference.message_id if message.reference else None
        timestamp = datetime.now().isoformat()
        attachments = [
            {
                "url": a.url,
                "filename": a.filename or "unknown",
                "size": a.size or 0,
                "content_type": a.content_type or "unknown"
            } for a in message.attachments if a.url
        ]
        author_name = str(message.author)
        channel_name = message.channel.name or 'Unknown'

        message_data = {
            "message_id": message.id,
            "timestamp": timestamp,
            "content": message.content,
            "parent_id": parent_id,
            "processed": False,
            "author_name": author_name,
            "channel_name": channel_name,
            "attachments": attachments
        }

        try:
            self.mongo_client.insert_message(self.messages_collection, message_data)
            sanitized_channel_name = sanitize_for_log(channel_name)
            logger.info(f"Stored message {message.id} from {sanitized_channel_name}.")
            
            logger.debug(f"Message {message.id}: {message.content}")
            if message.attachments:
                logger.debug(f"Attachments: {json.dumps(attachments, indent=2)}")
            
            try:
                redis_client.push_to_queue(QUEUE_NAME, str(message.id))
            except Exception as e:
                logger.error(f"Failed to push message {message.id} to Redis: {e}")
                await asyncio.sleep(2)
                redis_client.push_to_queue(QUEUE_NAME, str(message.id))
            
            logger.debug(f"Processed message {message.id} in {time.time() - start_time:.3f} seconds")
        except HTTPException as e:
            logger.error(f"Discord API error for message {message.id}: {e}")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Error with message {message.id}: {e}")

    async def on_disconnect(self):
        logger.warning("Discord client disconnected, attempting to reconnect")

    async def close(self):
        if getattr(self, 'closed', False):
            return
        logger.info("Closing MessageCollector")
        self.closed = True
        await super().close()
        self.mongo_client.close()
        redis_client.close()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    collector = MessageCollector(self_bot=True)
    try:
        collector.run(DISCORD_TOKEN)
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down")
        loop.run_until_complete(collector.close())
    except Exception as e:
        logger.error(f"Collector failed: {e}")
        loop.run_until_complete(collector.close())
    finally:
        loop.close()