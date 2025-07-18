import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

# Configure root logger once
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

# Avoid duplicate handlers
if not root_logger.handlers:
    # Console handler (shared)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)

def get_logger(name: str) -> logging.Logger:
    """Get a logger that inherits root config, with a rotating file handler if not present."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = True  # Ensure logs propagate to root/parent

    # Add file handler only if not already present (per logger/component)
    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        
        # Use component name for file (e.g., 'collector.log'), no timestamp
        log_file = os.path.join(logs_dir, f"{name.split('.')[-1]}.log")  # e.g., 'collector.log' even for submodules
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=5,  # Keep 5 backups
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
    
    return logger