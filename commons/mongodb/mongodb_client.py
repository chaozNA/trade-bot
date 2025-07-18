import os
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, OperationFailure
from dotenv import load_dotenv
from utils.logger import get_logger
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()
logger = get_logger('mongodb_client')

class MongoDBClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDBClient, cls).__new__(cls)
            atlas_connection = os.getenv("ATLAS_CONNECTION")
            if not atlas_connection:
                logger.error("ATLAS_CONNECTION environment variable is not set")
                raise ValueError("ATLAS_CONNECTION missing.")
            try:
                cls._instance.client = MongoClient(
                    atlas_connection,
                    maxPoolSize=10,
                    retryWrites=True,
                    wTimeoutMS=1000
                )
                cls._instance.db: Database = cls._instance.client["trade_bot"]
                logger.info("Connected to MongoDB Atlas")
            except ConnectionFailure as e:
                logger.error(f"Failed to connect to MongoDB Atlas: {e}")
                raise
        return cls._instance

    def get_collection(self, collection_name: str) -> Collection:
        return self.db[collection_name]

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(OperationFailure),
        before_sleep=lambda retry_state: logger.debug(
            f"Retrying MongoDB operation (attempt {retry_state.attempt_number})"
        )
    )
    def insert_message(self, collection: Collection, message_data: dict) -> None:
        try:
            collection.update_one(
                {"message_id": message_data["message_id"]},
                {"$set": message_data},
                upsert=True
            )
            logger.info(f"Stored message {message_data['message_id']} in {collection.name}")
        except OperationFailure as e:
            logger.error(f"Error storing message {message_data['message_id']}: {e}")
            raise

    def close(self) -> None:
        try:
            self.client.close()
            logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {e}")