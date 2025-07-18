# simulate_day.py:
#!/usr/bin/env python3
"""
Trade Bot Simulator

This script runs a complete simulation of the trade bot system using messages from the database.
It starts the processor and trade manager, then pushes messages from the database to Redis.
"""
import os
import sys
import time
import logging
import threading
from pathlib import Path

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import components
from processor.processor import MessageProcessor
from manager.trade_manager import TradeManager
from commons.redis.redis_client import redis_client
from commons.db.db_client import db_client
from utils.logging_config import setup_logging

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

# Constants
MESSAGE_QUEUE = os.getenv('MESSAGE_QUEUE', 'discord_messages')
TRADE_ACTIONS_QUEUE = os.getenv('TRADE_ACTIONS_QUEUE', 'trade_actions')
MESSAGE_BATCH_SIZE = 50  # Number of messages to process in each batch

class TradeBotSimulator:
    def __init__(self):
        self.running = False
        self.threads = []
        self.processor = None
        self.trade_manager = None
        
    def flush_queues(self):
        """Flush all Redis queues."""
        try:
            logger.info("Flushing all message queues...")
            redis_client.redis.delete(MESSAGE_QUEUE)
            redis_client.redis.delete(TRADE_ACTIONS_QUEUE)
            logger.info("All queues flushed successfully")
        except Exception as e:
            logger.error(f"Error flushing queues: {str(e)}", exc_info=True)
            raise
    
    def get_messages_from_db(self, limit: int = None) -> list:
        """Retrieve unprocessed messages from the database."""
        query = """
            SELECT * FROM messages 
            WHERE processed = 0 
            ORDER BY timestamp ASC
        """
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            messages = db_client.fetchall(query)
            logger.info(f"Retrieved {len(messages)} messages from database")
            return messages
        except Exception as e:
            logger.error(f"Error fetching messages: {str(e)}", exc_info=True)
            return []
    
    def mark_message_processed(self, message_id: str):
        """Mark a message as processed in the database."""
        try:
            db_client.execute(
                "UPDATE messages SET processed = 1 WHERE message_id = ?",
                (message_id,)
            )
        except Exception as e:
            logger.error(f"Error marking message {message_id} as processed: {str(e)}")
    
    def run_simulation(self):
        """Run the complete simulation."""
        while self.running:
            messages = self.get_messages_from_db(limit=MESSAGE_BATCH_SIZE)
            if not messages:
                logger.info("No more unprocessed messages found")
                break
            
            for msg in messages:
                if not self.running:
                    break
                
                try:
                    # Push to Redis
                    redis_client.push_to_queue(MESSAGE_QUEUE, msg)
                    logger.debug(f"Pushed message {msg['message_id']} to {MESSAGE_QUEUE}")
                    
                    # Mark as processed
                    self.mark_message_processed(msg['message_id'])
                    
                    # Add delay between messages for realistic processing
                    time.sleep(5)
                    
                except Exception as e:
                    logger.error(f"Error processing message {msg.get('message_id')}: {str(e)}", exc_info=True)

    def start(self):
        """Start all components of the trade bot."""
        try:
            self.running = True
            
            # Flush queues first
            self.flush_queues()
            
            # Initialize components
            self.processor = MessageProcessor()
            self.trade_manager = TradeManager()
            
            # Start processor thread
            processor_thread = threading.Thread(target=self.processor.run, daemon=True)
            processor_thread.start()
            self.threads.append(processor_thread)
            logger.info("Started processor thread")
            
            # Start trade manager thread
            manager_thread = threading.Thread(target=self.trade_manager.run, daemon=True)
            manager_thread.start()
            self.threads.append(manager_thread)
            logger.info("Started trade manager thread")
            
            # Give components time to initialize
            time.sleep(2)
            
            # Run simulation in the main thread
            self.run_simulation()
            
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        except Exception as e:
            logger.critical(f"Fatal error: {str(e)}", exc_info=True)
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Shut down all components."""
        self.running = False
        logger.info("Shutting down trade bot...")
        
        # Wait for threads to finish
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=5)
        
        logger.info("Trade bot shutdown complete")

def main():
    """Main entry point."""
    # Start the trade bot simulator
    bot = TradeBotSimulator()
    bot.start()

if __name__ == "__main__":
    main()