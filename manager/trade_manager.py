import os
import json
import logging
import uuid
from commons.redis.redis_client import redis_client
from commons.db.db_client import db_client
from typing import Dict, Optional

# Constants
TRADE_ACTIONS_QUEUE = os.getenv('TRADE_ACTIONS_QUEUE', 'trade_actions')

# Map descriptive sizing to numerical quantity
SIZING_MAP = {
    'small': 1,
    'medium': 5,
    'large': 10
}

logger = logging.getLogger(__name__)

class TradeManager:
    def __init__(self):
        self.redis_client = redis_client
        self.db_client = db_client
        logger.info("Trade Manager initialized")

    def get_analysis_by_message_id(self, message_id: str) -> Optional[Dict]:
        """Fetches the full analysis record from the database."""
        query = "SELECT * FROM message_analyses WHERE message_id = ?"
        return self.db_client.fetchone(query, (message_id,))

    def process_action(self, action_data: dict):
        """Process a trade action by fetching the definitive analysis from the DB."""
        try:
            message_id = action_data.get('message_id')
            if not message_id:
                logger.error(f"Received action with no message_id: {action_data}")
                return

            logger.info(f"Processing action for message_id: {message_id}")
            analysis_record = self.get_analysis_by_message_id(message_id)

            if not analysis_record:
                logger.error(f"Could not find analysis for message_id: {message_id}. Might be a race condition.")
                return

            # Extract data directly from the analysis record's columns
            classification = analysis_record.get('classification')
            analysis_id = analysis_record.get('analysis_id')
            reason = analysis_record.get('reason')
            confidence = analysis_record.get('confidence_score')
            related_trade_id = analysis_record.get('related_trade_id')
            payload_str = analysis_record.get('analysis_payload', '{}')
            payload = json.loads(payload_str)  # The full payload is now just the analysis

            # Pass the full context to the handlers
            action_context = {
                'analysis_id': analysis_id,
                'reason': reason,
                'confidence': confidence,
                'related_trade_id': related_trade_id,
                'payload': payload
            }

            # Dispatch to the correct handler based on the classification
            if classification == 'new_trade':
                self.open_trade(action_context)
            elif classification == 'trade_update':
                self.update_trade(action_context)
            elif classification == 'trade_close':
                self.close_trade(action_context)
            elif classification in ['irrelevant', 'other']:
                logger.info(f"Ignoring action with classification '{classification}' for message {message_id}")
            else:
                logger.warning(f"Unknown classification '{classification}' for message {message_id}")

        except Exception as e:
            logger.error(f"Error processing trade action for message {message_id}: {e}", exc_info=True)

    def open_trade(self, context: Dict):
        """Create a new trade based on a 'new_trade' analysis."""
        analysis_id = context['analysis_id']
        analysis_payload = context['payload']
        try:
            client_order_id = f"bot-{uuid.uuid4()}"
            logger.info(f"Opening new trade for analysis {analysis_id} with client_order_id {client_order_id}")

            query = '''
                INSERT INTO trades (
                    opening_analysis_id, client_order_id, symbol, option_type, strike, 
                    expiration, status, quantity, target_entry_price, stop_loss, take_profit
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            # Convert sizing to quantity, defaulting to 'small'
            sizing = analysis_payload.get('sizing', 'small')
            quantity = SIZING_MAP.get(sizing, 1)

            params = (
                analysis_id,
                client_order_id,
                analysis_payload.get('ticker'),
                analysis_payload.get('option_type'),
                analysis_payload.get('strike_price'),
                analysis_payload.get('expiration_date'),
                'pending_open',
                quantity,
                analysis_payload.get('entry_price'),
                analysis_payload.get('stop_loss'),
                analysis_payload.get('take_profit')
            )

            trade_id = self.db_client.insert_and_get_id(query, params)

            if trade_id:
                logger.info(f"New trade {trade_id} created successfully.")
                history_details = {
                    'reason': context.get('reason'),
                    'confidence': context.get('confidence'),
                    'full_payload': analysis_payload
                }
                self._add_trade_history(trade_id, analysis_id, 'create', history_details)
            else:
                raise ValueError(f"Failed to insert new trade for analysis {analysis_id}")

        except Exception as e:
            logger.error(f"Error opening trade for analysis {analysis_id}: {e}", exc_info=True)

    def update_trade(self, context: Dict):
        """Update an existing trade's parameters (e.g., stop-loss, take-profit, trim/add quantity)."""
        analysis_id = context['analysis_id']
        trade_id = context['related_trade_id']
        analysis_payload = context['payload']
        if not trade_id:
            logger.error(f"Update failed: analysis {analysis_id} did not have a related_trade_id.")
            return
        try:
            logger.info(f"Updating trade {trade_id} based on analysis {analysis_id}")
            trade = self.db_client.fetchone("SELECT * FROM trades WHERE trade_id = ?", (trade_id,))
            if not trade:
                logger.error(f"Cannot update: Trade {trade_id} not found.")
                return

            updates = []
            params = {'trade_id': trade_id}
            # Start with new rich context from the analysis record
            history_details = {
                'reason': context.get('reason'),
                'confidence': context.get('confidence'),
                'updates': {}
            }

            # Check for updates to stop_loss or take_profit
            for field in ['stop_loss', 'take_profit']:
                new_value = analysis_payload.get(field)
                if new_value is not None and new_value != trade.get(field):
                    updates.append(f"{field} = :{field}")
                    params[field] = new_value
                    history_details['updates'][field] = {'old': trade.get(field), 'new': new_value}

            # Handle quantity changes (e.g., trim or add)
            details = analysis_payload.get('details', '').lower()
            current_quantity = trade.get('quantity', 0)
            quantity_change = 0
            if 'trim' in details or 'scale out' in details:
                # Parse trim amount (e.g., "trim 50%" or "trim 2")
                if '%' in details:
                    percent = int(details.split('%')[0].split()[-1]) / 100
                    quantity_change = -int(current_quantity * percent)
                else:
                    # Assume numeric trim, e.g., "trim 3"
                    try:
                        quantity_change = -int(details.split()[-1])
                    except ValueError:
                        quantity_change = -1  # Default trim 1 if unclear
                history_details['updates']['quantity'] = {'old': current_quantity, 'change': quantity_change}
            elif 'add' in details or 'scale in' in details:
                # Similar parsing for adding
                if '%' in details:
                    percent = int(details.split('%')[0].split()[-1]) / 100
                    quantity_change = int(current_quantity * percent)
                else:
                    try:
                        quantity_change = int(details.split()[-1])
                    except ValueError:
                        quantity_change = 1  # Default add 1
                history_details['updates']['quantity'] = {'old': current_quantity, 'change': quantity_change}

            if quantity_change != 0:
                new_quantity = max(0, current_quantity + quantity_change)  # Prevent negative
                updates.append("quantity = :quantity")
                params['quantity'] = new_quantity
                history_details['updates']['quantity']['new'] = new_quantity

            # Also log any other details from the payload
            if details:
                history_details['details'] = details

            if updates or 'details' in history_details:
                if updates:
                    update_query = f"""
                        UPDATE trades SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP
                        WHERE trade_id = :trade_id
                    """
                    self.db_client.execute(update_query, params)
                
                # Check if trade should be closed (quantity <= 0)
                updated_trade = self.db_client.fetchone("SELECT quantity FROM trades WHERE trade_id = ?", (trade_id,))
                if updated_trade['quantity'] <= 0:
                    self.close_trade({'related_trade_id': trade_id, 'analysis_id': analysis_id, 'reason': 'Quantity reached zero after update', 'payload': {}})

                logger.info(f"Trade {trade_id} updated successfully: {history_details}")
                self._add_trade_history(trade_id, analysis_id, 'update', history_details)
            else:
                logger.info(f"No updatable fields or details found in analysis {analysis_id} for trade {trade_id}")

        except Exception as e:
            logger.error(f"Error updating trade {trade_id}: {e}", exc_info=True)

    def close_trade(self, context: Dict):
        """Close an existing trade by updating its status and setting quantity to 0."""
        analysis_id = context['analysis_id']
        trade_id = context['related_trade_id']
        analysis_payload = context['payload']
        if not trade_id:
            logger.error(f"Close failed: analysis {analysis_id} did not have a related_trade_id.")
            return
        try:
            logger.info(f"Closing trade {trade_id} based on analysis {analysis_id}")
            query = """
                UPDATE trades 
                SET status = 'closed', quantity = 0, closed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE trade_id = ? AND status != 'closed'
            """
            self.db_client.execute(query, (trade_id,))
            logger.info(f"Trade {trade_id} closed successfully.")
            history_details = {
                'reason': context.get('reason'),
                'confidence': context.get('confidence'),
                'full_payload': analysis_payload
            }
            self._add_trade_history(trade_id, analysis_id, 'close', history_details)

        except Exception as e:
            logger.error(f"Error closing trade {trade_id}: {e}", exc_info=True)

    def _add_trade_history(self, trade_id: int, analysis_id: int, event_type: str, details: Dict):
        """Add a complete audit event to the trade history table."""
        try:
            query = """
                INSERT INTO trade_history (trade_id, triggering_analysis_id, event_type, details)
                VALUES (?, ?, ?, ?)
            """
            details_json = json.dumps(details, default=str)
            self.db_client.execute(query, (trade_id, analysis_id, event_type, details_json))
            logger.debug(f"Logged '{event_type}' for trade {trade_id}, triggered by analysis {analysis_id}")
        except Exception as e:
            logger.error(f"Failed to log history for trade {trade_id}: {e}", exc_info=True)

    def run(self):
        """Main run loop for the trade manager.""" 
        logger.info("Starting TradeManager")
        while True:
            try:
                # Get trade actions from Redis queue
                action_data = self.redis_client.pop_from_queue(
                    TRADE_ACTIONS_QUEUE,
                    timeout=1,
                    is_json=True
                )
                
                if action_data:
                    self.process_action(action_data)
                    
            except Exception as e:
                logger.error(f"Error in trade manager: {str(e)}", exc_info=True)