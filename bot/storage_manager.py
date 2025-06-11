import json
import logging
from pathlib import Path
from typing import Dict, List

from .trade import Trade

class StorageManager:
    """Handles saving and loading of trade data to a JSON file."""

    def __init__(self, storage_path: str = 'data/trades.json'):
        self.storage_file = Path(storage_path)
        self.storage_file.parent.mkdir(parents=True, exist_ok=True)

    def save_trades(self, trades: Dict[int, Trade]):
        """Saves the current state of all trades to the JSON file."""
        try:
            trade_data = {
                message_id: trade.to_dict() 
                for message_id, trade in trades.items()
            }
            with open(self.storage_file, 'w') as f:
                json.dump(trade_data, f, indent=4)
            logging.info(f"Successfully saved {len(trades)} trades to {self.storage_file}")
        except Exception as e:
            logging.error(f"Failed to save trades: {e}")

    def load_trades(self) -> Dict[int, Trade]:
        """Loads trades from the JSON file into memory."""
        if not self.storage_file.exists():
            logging.warning("Storage file not found. Starting with no active trades.")
            return {}
        
        try:
            with open(self.storage_file, 'r') as f:
                trades_data = json.load(f)
            
            loaded_trades = {
                int(message_id): Trade.from_dict(data)
                for message_id, data in trades_data.items()
            }
            logging.info(f"Successfully loaded {len(loaded_trades)} trades from {self.storage_file}")
            return loaded_trades
        except (json.JSONDecodeError, TypeError) as e:
            logging.error(f"Failed to load or parse trades, starting fresh: {e}")
            return {}
