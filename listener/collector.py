import asyncio
import sqlite3
import discord
from datetime import datetime
import json
import os
from typing import Set, Optional
from dotenv import load_dotenv
from commons.redis.redis_client import redis_client
from utils.logging_config import setup_logging

# Load environment variables
load_dotenv()

# Initialize logging
logger = setup_logging('collector')

# Load configuration
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_IDS = {cid for cid in os.getenv("DISCORD_CHANNEL_IDS", "").split(",") if cid}
QUEUE_NAME = os.getenv("MESSAGE_QUEUE_NAME", "message_queue")

if not DISCORD_TOKEN:
    logger.error("DISCORD_TOKEN environment variable is not set")
    raise ValueError("DISCORD_TOKEN missing.")

# Set up database path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT_DIR, 'data', 'bot.db')

logger.info(f"Starting message collector for channels: {', '.join(CHANNEL_IDS) or 'None'}")

def init_db():
    """Initialize the SQLite database with required tables."""
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        # Create messages table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                message_id INTEGER PRIMARY KEY,
                timestamp TEXT NOT NULL,
                discord_timestamp TEXT,
                content TEXT,
                parent_id INTEGER,
                processed BOOLEAN DEFAULT FALSE,
                author_id INTEGER,
                channel_id INTEGER,
                attachments TEXT
            )
        ''')
        
        # Create authors table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS authors (
                author_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        ''')
        
        # Create channels table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                channel_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        ''')
        
        conn.commit()
        logger.info("Database initialized successfully")
    except sqlite3.Error as e:
        logger.error(f"Error initializing database: {e}")
        raise
    finally:
        if conn:
            conn.close()

def update_author(author_id: int, author_name: str, conn: sqlite3.Connection) -> None:
    """Update or insert an author in the database."""
    try:
        cur = conn.cursor()
        cur.execute('''
            INSERT OR IGNORE INTO authors (author_id, name) 
            VALUES (?, ?)
        ''', (author_id, author_name))
        conn.commit()
        logger.debug(f"Updated author: {author_name} ({author_id})")
    except sqlite3.Error as e:
        logger.error(f"Error updating author {author_id}: {e}")
        raise

def update_channel(channel_id: int, channel_name: str, conn: sqlite3.Connection) -> None:
    """Update or insert a channel in the database."""
    try:
        cur = conn.cursor()
        cur.execute('''
            INSERT OR IGNORE INTO channels (channel_id, name) 
            VALUES (?, ?)
        ''', (channel_id, channel_name))
        conn.commit()
        logger.debug(f"Updated channel: {channel_name} ({channel_id})")
    except sqlite3.Error as e:
        logger.error(f"Error updating channel {channel_id}: {e}")
        raise

class MessageCollector(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger('collector')

    async def on_ready(self):
        """Called when the client is done preparing the data from Discord."""
        self.logger.info(f"Logged in as {self.user} (ID: {self.user.id})")
        self.logger.info(f"Monitoring {len(CHANNEL_IDS)} channel(s)")

    async def on_message(self, message: discord.Message):
        """Called when a message is created and seen by the client."""
        # Skip messages from bots and ignored channels
        if message.author.bot:
            return
            
        if str(message.channel.id) not in CHANNEL_IDS:
            self.logger.debug(f"Ignoring message from non-monitored channel: {message.channel.name} ({message.channel.id})")
            return
        
        parent_id = message.reference.message_id if message.reference else None
        timestamp = datetime.now().isoformat()
        discord_timestamp = str(message.created_at)
        author_id = message.author.id
        channel_id = message.channel.id
        attachments_json = json.dumps([a.url for a in message.attachments])
        author_name = str(message.author)
        channel_name = message.channel.name or 'Unknown'

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        try:
            update_author(author_id, author_name, conn)
            update_channel(channel_id, channel_name, conn)
            
            cur.execute('''
                INSERT OR REPLACE INTO messages 
                (message_id, timestamp, discord_timestamp, content, parent_id, processed, author_id, channel_id, attachments)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (message.id, timestamp, discord_timestamp, message.content, parent_id, False, author_id, channel_id, attachments_json))
            conn.commit()
            logging.info(f"Stored message {message.id} from {channel_name}.")
            
            logging.debug(f"Message {message.id}: {message.content}")
            if message.attachments:
                logging.debug(f"Attachments: {attachments_json}")
            
            redis_client.push_to_queue(QUEUE_NAME, str(message.id))
        except Exception as e:
            logging.error(f"Error with message {message.id}: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    init_db()
    collector = MessageCollector(self_bot=True)
    collector.run(DISCORD_TOKEN)