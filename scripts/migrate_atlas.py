import sqlite3
import os
import json
from datetime import datetime
from commons.mongodb.mongodb_client import MongoDBClient
from utils.logger import get_logger
from pymongo.errors import OperationFailure
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = get_logger('migration')

# Resolve path from scripts/ to project root
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT_DIR, 'data', 'bot.db')
MESSAGES_COLLECTION = os.getenv("MESSAGES_COLLECTION", "raw_messages")
ANALYZED_ACTIONS_COLLECTION = os.getenv("ANALYZED_ACTIONS_COLLECTION", "analyzed_actions")
COUNTERS_COLLECTION = os.getenv("COUNTERS_COLLECTION", "counters")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(OperationFailure),
    before_sleep=lambda retry_state: logger.debug(
        f"Retrying MongoDB operation (attempt {retry_state.attempt_number})"
    )
)
def create_indexes(collection, index_field):
    collection.create_index(index_field, unique=True)
    logger.info(f"Created unique index on {index_field} for {collection.name}")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(OperationFailure),
    before_sleep=lambda retry_state: logger.debug(
        f"Retrying MongoDB operation (attempt {retry_state.attempt_number})"
    )
)
def initialize_counter(collection, counter_name, initial_value):
    collection.update_one(
        {"_id": counter_name},
        {"$set": {"sequence_value": initial_value}},
        upsert=True
    )
    logger.info(f"Initialized counter {counter_name} with value {initial_value}")

def migrate_messages():
    mongo_client = MongoDBClient()
    raw_messages_collection = mongo_client.get_collection(MESSAGES_COLLECTION)
    analyzed_actions_collection = mongo_client.get_collection(ANALYZED_ACTIONS_COLLECTION)
    counters_collection = mongo_client.get_collection(COUNTERS_COLLECTION)

    try:
        create_indexes(raw_messages_collection, "message_id")
        create_indexes(analyzed_actions_collection, "message_id")

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        logger.info("Starting migration of messages table")
        cur.execute("""
            SELECT m.message_id, m.timestamp, m.content, m.parent_id, m.processed, m.attachments,
                   a.name AS author_name, c.name AS channel_name
            FROM messages m
            LEFT JOIN authors a ON m.author_id = a.author_id
            LEFT JOIN channels c ON m.channel_id = c.channel_id
        """)
        messages = cur.fetchall()
        for msg in messages:
            try:
                attachments = json.loads(msg["attachments"]) if msg["attachments"] else []
                new_attachments = [
                    {
                        "url": url,
                        "filename": "unknown",
                        "size": 0,
                        "content_type": "unknown"
                    } for url in attachments if url
                ]
                message_data = {
                    "message_id": msg["message_id"],
                    "timestamp": msg["timestamp"],
                    "content": msg["content"],
                    "parent_id": msg["parent_id"],
                    "processed": bool(msg["processed"]),
                    "author_name": msg["author_name"] or "Unknown",
                    "channel_name": msg["channel_name"] or "Unknown",
                    "attachments": new_attachments
                }
                mongo_client.insert_message(raw_messages_collection, message_data)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse attachments for message {msg['message_id']}: {e}")
                message_data = {
                    "message_id": msg["message_id"],
                    "timestamp": msg["timestamp"],
                    "content": msg["content"],
                    "parent_id": msg["parent_id"],
                    "processed": bool(msg["processed"]),
                    "author_name": msg["author_name"] or "Unknown",
                    "channel_name": msg["channel_name"] or "Unknown",
                    "attachments": []
                }
                mongo_client.insert_message(raw_messages_collection, message_data)

        logger.info("Starting migration of message_analyses table")
        cur.execute("""
            SELECT analysis_id, message_id, classification, related_trade_id, reason, 
                   confidence_score, analysis_payload, created_at
            FROM message_analyses
        """)
        analyses = cur.fetchall()
        max_analysis_id = 0
        for analysis in analyses:
            try:
                analysis_payload = json.loads(analysis["analysis_payload"]) if analysis["analysis_payload"] else {}
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse analysis_payload for analysis {analysis['analysis_id']}: {e}")
                analysis_payload = {}
            analysis_data = {
                "analysis_id": analysis["analysis_id"],
                "message_id": analysis["message_id"],
                "action_type": analysis["classification"] or "",
                "related_trade_id": analysis["related_trade_id"],
                "reason": analysis["reason"] or "",
                "confidence_score": float(analysis["confidence_score"] or 0.0),
                "analysis_payload": analysis_payload,
                "analysis_timestamp": analysis["created_at"] or datetime.now().isoformat()
            }
            mongo_client.insert_message(analyzed_actions_collection, analysis_data)
            max_analysis_id = max(max_analysis_id, analysis["analysis_id"] or 0)

        initialize_counter(counters_collection, "analysis_id", max_analysis_id)
        
        logger.info("Migration completed successfully")
    except (OperationFailure, sqlite3.Error) as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        conn.close()
        mongo_client.close()

if __name__ == "__main__":
    migrate_messages()