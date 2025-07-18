# No major changes, but added type hints and comments for readability
import redis
import json
import logging
from typing import Any, Dict, Callable, Optional
from dotenv import load_dotenv
import os
import time

load_dotenv()

class RedisClient:
    def __init__(self):
        host = os.getenv('REDIS_HOST', 'localhost')
        port = int(os.getenv('REDIS_PORT', 6379))
        db = int(os.getenv('REDIS_DB', '0'))
        password = os.getenv('REDIS_PASSWORD', None)
        
        self.redis = redis.Redis(
            host,
            port=port,
            db=db,
            password=password,
            decode_responses=True,
            retry_on_timeout=True
        )
        self.pubsub = self.redis.pubsub()
        self.logger = logging.getLogger(__name__)
        
        try:
            self.redis.ping()
            self.logger.info("Redis connected.")
        except redis.ConnectionError as e:
            self.logger.error(f"Redis connection failed: {e}")
            raise

    def push_to_queue(self, queue_name: str, value: Any) -> None:
        """Push value to Redis list (queue). Auto-JSON if dict."""
        try:
            if isinstance(value, dict):
                value = json.dumps(value)
            self.redis.lpush(queue_name, value)
            self.logger.debug(f"Pushed to {queue_name}: {value}")
        except Exception as e:
            self.logger.error(f"Error pushing to {queue_name}: {e}")

    def pop_from_queue(self, queue_name: str, timeout: int = 0, is_json: bool = False) -> Optional[Any]:
        """Pop from Redis list with timeout. Parse JSON if is_json."""
        try:
            result = self.redis.brpop(queue_name, timeout=timeout)
            if result:
                _, value = result
                if is_json:
                    return json.loads(value)
                return value
            return None
        except Exception as e:
            self.logger.error(f"Error popping from {queue_name}: {e}")
            return None

# Singleton
redis_client = RedisClient()