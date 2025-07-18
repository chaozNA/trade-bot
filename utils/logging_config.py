import logging
import logging.handlers
from datetime import datetime
import random
import string
import json
import os
from typing import Dict, Any, Optional

# Constants
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
LOG_FORMAT = '%(asctime)s [%(levelname)-8s] [%(name)s.%(funcName)s:%(lineno)d] %(message)s'
LOG_DIR = 'logs'
LOG_BASE = 'trade_bot'
LOG_EXT = 'log'
MAX_BYTES = 10 * 1024 * 1024  # 10MB
BACKUP_COUNT = 7

# Generate log file name with date
def get_log_file_path():
    """Generate log file path with current date."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    unique = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return os.path.join(LOG_DIR, f"{LOG_BASE}_{date_str}_{unique}.{LOG_EXT}")

def setup_logging():
    """
    Configure the root logger with file and console handlers.
    Call this once at application startup.
    """
    # Ensure logs directory exists
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # Clear any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Set log level
    root_logger.setLevel(LOG_LEVEL)
    
    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT)
    
    # File handler with daily rotation
    log_file = get_log_file_path()
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Add handlers to root logger
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Configure third-party loggers
    for logger_name in ['urllib3', 'asyncio', 'websockets']:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    
    logging.info(f"Logging initialized at level {LOG_LEVEL}")

def log_api_call(
    logger: logging.Logger,
    endpoint: str,
    method: str = 'POST',
    request_data: Optional[Dict] = None,
    response_data: Optional[Dict] = None,
    status_code: Optional[int] = None,
    duration: Optional[float] = None,
    error: Optional[Exception] = None
):
    """Log API call details in a structured format as JSON message."""
    data = {
        'event': 'api_call',
        'endpoint': endpoint,
        'method': method,
        'request': request_data or {},
        'response': response_data or {},
        'status_code': status_code,
        'duration_ms': round(duration * 1000, 2) if duration else None,
    }
    
    if error:
        data['error'] = str(error)
        msg = json.dumps(data)
        logger.error(msg)
    else:
        msg = json.dumps(data)
        logger.info(msg)