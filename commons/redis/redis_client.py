import redis
import json
import os
from typing import Any, Dict, Optional
from dotenv import load_dotenv
from utils.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()
logger = get_logger('redis_client')

class RedisClient:
    def __init__(self):
        host = os.getenv('REDIS_HOST', 'localhost')
        port = int(os.getenv('REDIS_PORT', 6379))
        db = int(os.getenv('REDIS_DB', '0'))
        password = os.getenv('REDIS_PASSWORD', None)
        
        try:
            self.redis = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=True,
                retry_on_timeout=True,
                max_connections=10
            )
            self.pubsub = self.redis.pubsub()
            self.redis.ping()
            logger.info("Redis connected.")
        except redis.ConnectionError as e:
            logger.error(f"Redis connection failed: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(redis.ConnectionError),
        before_sleep=lambda retry_state: logger.debug(
            f"Retrying Redis operation (attempt {retry_state.attempt_number})"
        )
    )
    def push_to_queue(self, queue_name: str, value: Any) -> None:
        try:
            if isinstance(value, dict):
                value = json.dumps(value)
            self.redis.lpush(queue_name, value)
            logger.debug(f"Pushed to {queue_name}: {value}")
        except redis.ConnectionError as e:
            logger.error(f"Error pushing to {queue_name}: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(redis.ConnectionError),
        before_sleep=lambda retry_state: logger.debug(
            f"Retrying Redis operation (attempt {retry_state.attempt_number})"
        )
    )
    def pop_from_queue(self, queue_name: str, timeout: int = 0, is_json: bool = False) -> Optional[Any]:
        try:
            result = self.redis.brpop(queue_name, timeout=timeout)
            if result:
                _, value = result
                if is_json:
                    return json.loads(value)
                return value
            return None
        except redis.ConnectionError as e:
            logger.error(f"Error popping from {queue_name}: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(redis.ConnectionError),
        before_sleep=lambda retry_state: logger.debug(
            f"Retrying Redis operation (attempt {retry_state.attempt_number})"
        )
    )
    def publish(self, channel: str, message: Any) -> None:
        try:
            if isinstance(message, dict):
                message = json.dumps(message)
            self.redis.publish(channel, message)
            logger.debug(f"Published to {channel}: {message}")
        except redis.ConnectionError as e:
            logger.error(f"Error publishing to {channel}: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(redis.ConnectionError),
        before_sleep=lambda retry_state: logger.debug(
            f"Retrying Redis operation (attempt {retry_state.attempt_number})"
        )
    )
    def subscribe(self, channel: str) -> redis.client.PubSub:
        try:
            self.pubsub.subscribe(channel)
            logger.info(f"Subscribed to {channel}")
            return self.pubsub
        except redis.ConnectionError as e:
            logger.error(f"Error subscribing to {channel}: {e}")
            raise

    def close(self) -> None:
        try:
            self.pubsub.close()
            self.redis.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

# Singleton
redis_client = RedisClient()