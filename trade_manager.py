import logging
import re
from typing import Optional

from datetime import datetime, timedelta
from trade import Trade, TradeStatus, TradeType
from storage_manager import StorageManager


class TradeManager:
    """Manages all trades and processes messages to update trade states."""

    def __init__(self, storage_manager: StorageManager):
        self.storage = storage_manager
        self.active_trades: dict[int, Trade] = self.storage.load_trades()
        self.trade_history: list[Trade] = [] # Could also be loaded if needed

    def process_message(self, message_content: str, message_id: int, parent_message_id: Optional[int] = None):
        """Processes a new message and updates trade states."""
        logging.info(f"Processing message_id: {message_id} | parent_id: {parent_message_id}")
        
        # 1. Check if it's an update to an existing trade by looking at the parent message
        if parent_message_id and parent_message_id in self.active_trades:
            self._handle_trade_update(message_content, parent_message_id)
            return

        # 2. Check if it's a new trade alert
        if ":RedAlert:" in message_content:
            trade = self._parse_new_trade(message_content, message_id)
            if trade:
                self.active_trades[trade.message_id] = trade
                logging.info(f"✅ New trade opened: {trade}")
                self._save_all_trades()
                return

        # 3. Check if it's an update to an existing trade by context (no reply)
        self._handle_contextual_update(message_content)


    def _parse_new_trade(self, message_content: str, message_id: int) -> Optional[Trade]:
        """Parses a new trade alert and creates a Trade object."""
        try:
            # Extract symbol
            match = re.search(r":RedAlert:\s*([A-Z]+)", message_content)
            if not match:
                return None
            symbol = match.group(1)

            trade = Trade(message_id=message_id, symbol=symbol)

            # Extract trade type (CALLS/PUTS)
            if "CALLS" in message_content.upper():
                trade.trade_type = TradeType.CALL
            elif "PUTS" in message_content.upper():
                trade.trade_type = TradeType.PUT

            # Extract strike price
            match = re.search(r"\$?(\d{2,4})\s*(?:CALLS|PUTS)", message_content, re.IGNORECASE)
            if match:
                trade.strike_price = float(match.group(1))

            # Extract entry price
            prices = re.findall(r"\$?(\d*\.\d+|\d+)", message_content)
            if prices:
                if trade.strike_price:
                    try:
                        strike_str = str(int(trade.strike_price))
                        if strike_str in message_content:
                            parts = message_content.split(strike_str)
                            entry_prices = re.findall(r"\$?(\d*\.\d+)", parts[1])
                            if entry_prices:
                                trade.entry_price = float(entry_prices[0])
                    except (ValueError, IndexError):
                        pass

            # Extract stop loss
            match = re.search(r"STOP\s*LOSS\s*AT\s*\$?(\d*\.\d+|\d+)", message_content, re.IGNORECASE)
            if match:
                trade.stop_loss = float(match.group(1))

            # Extract expiration
            match = re.search(r"EXPIRATION\s*([\w\s]+?)\s*\$?", message_content, re.IGNORECASE)
            if match:
                trade.expiration_date = match.group(1).strip()
            elif "0DTE" in message_content:
                trade.expiration_date = "0DTE"

            return trade
        except Exception as e:
            logging.error(f"Error parsing new trade: {e}")
            return None

    def _handle_trade_update(self, message_content: str, trade_message_id: int):
        """Handles an update to an existing trade."""
        trade = self.active_trades.get(trade_message_id)
        if not trade:
            return
        
        logging.info(f"Updating trade {trade.symbol} based on: {message_content}")
        trade.updates.append(message_content)
        
        message_upper = message_content.upper()

        if "TRIM" in message_upper or "TOOK" in message_upper:
            trade.status = TradeStatus.TRIMMED
            logging.info(f"Status for {trade.symbol} updated to TRIMMED")
        elif "OUT" in message_upper or "CLOSE" in message_upper or "HIT SL" in message_upper:
            trade.status = TradeStatus.CLOSED
            logging.info(f"Status for {trade.symbol} updated to CLOSED")

        trade.last_update_time = datetime.now()
        self._save_all_trades()

    def _handle_contextual_update(self, message_content: str):
        """Handles updates that are not direct replies, first by ticker, then by time."""
        # 1. Try to find a trade based on a ticker in the message
        symbols_in_message = re.findall(r"\b([A-Z]{2,5})\b", message_content)
        if symbols_in_message:
            target_symbol = symbols_in_message[0]
            latest_trade_for_symbol: Optional[Trade] = None
            for trade in reversed(list(self.active_trades.values())):
                if trade.symbol == target_symbol:
                    latest_trade_for_symbol = trade
                    break
            if latest_trade_for_symbol:
                logging.info(f"Found contextual update for {target_symbol} by ticker.")
                self._handle_trade_update(message_content, latest_trade_for_symbol.message_id)
                return

        # 2. If no ticker found, fall back to the most recently updated trade
        if not self.active_trades:
            return

        latest_trade = max(self.active_trades.values(), key=lambda t: t.last_update_time)

        # Only apply if the last update was recent (e.g., within 15 minutes)
        if datetime.now() - latest_trade.last_update_time < timedelta(minutes=15):
            logging.info(f"Found contextual update for {latest_trade.symbol} by time.")
            self._handle_trade_update(message_content, latest_trade.message_id)
        else:
            logging.info("Contextual message received, but no recent trade to apply it to.")

    def _save_all_trades(self):
        """Saves all active trades to the storage."""
        self.storage.save_trades(self.active_trades)
