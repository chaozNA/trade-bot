import os
import json
import time
import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
from dotenv import load_dotenv
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential
from commons.redis.redis_client import redis_client
from commons.db.db_client import db_client
from utils.logging_config import setup_logging, log_api_call

# Set up logging
logger = logging.getLogger(__name__)

load_dotenv()
MESSAGE_QUEUE = os.getenv('MESSAGE_QUEUE', 'discord_messages')
TRADE_ACTIONS_QUEUE = os.getenv('TRADE_ACTIONS_QUEUE', 'trade_actions')
XAI_API_KEY = os.getenv('XAI_API_KEY')

def parse_expiration_date(date_str: str) -> str:
    """Parse various string representations of expiration dates into YYYY-MM-DD format."""
    if not isinstance(date_str, str):
        return date.today().isoformat() # Default to today if invalid input

    date_str = date_str.lower()
    today = date.today()

    if 'this week' in date_str or 'end of week' in date_str:
        # Find the next Friday
        friday = today + timedelta(days=(4 - today.weekday() + 7) % 7)
        return friday.isoformat()
    if '0dte' in date_str or 'today' in date_str:
        return today.isoformat()
    if 'tomorrow' in date_str:
        return (today + timedelta(days=1)).isoformat()
    
    # Attempt to parse with common formats
    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y', '%b %d, %Y'):
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
            
    # Default to today if all parsing fails
    return today.isoformat()

class GrokAnalyzer:
    def __init__(self):
        # Get the logger for this specific module and set its level to DEBUG
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.grok_client = OpenAI(api_key=XAI_API_KEY, base_url="https://api.x.ai/v1") if XAI_API_KEY else None
        if not self.grok_client:
            raise ValueError("XAI_API_KEY missing")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def analyze_message(self, content: str, history: List[Dict], reply_chain: str, active_trades: List[Dict]) -> Tuple[Dict, Dict, Optional[str]]:
        """Analyze a message using the Grok API and return the structured analysis."""
        if not self.grok_client:
            return {'classification': 'other', 'reason': 'Grok client not initialized'}, {}, None

        self.logger.debug(f"Analyzing message content: {content}")
        try:
            # Prepare context for the prompt
            active_trades_str = json.dumps(active_trades, indent=2, default=str)
            message_content = content
            reply_chain_str = reply_chain
            current_date_str = date.today().isoformat()

            # Concise and clear prompt for consistent LLM responses
            prompt = f"""
You are an expert trading analysis bot. Analyze the given Discord message and classify it as one of: 
- 'new_trade': New trade with ticker, option_type (CALL/PUT), strike_price, expiration_date.
- 'trade_update': Update to an active trade (e.g., trim, stop-loss change).
- 'trade_close': Complete exit from an active trade.
- 'irrelevant': No actionable trade info (e.g., general chat).

**Rules**:
- Classify as 'new_trade' ONLY if message has explicit details: ticker, CALL/PUT, strike, expiration. Else, 'irrelevant'.
- For 'trade_update' or 'trade_close', MUST reference an active trade by symbol or details; include 'related_trade_id' (exact integer from active trades). If no match, classify as 'irrelevant'. Example: If message says "trim ORCL" and active trades have {{"trade_id": 456, "symbol": "ORCL"}}, use 456.
- Infer 'sizing' for new trades: 'small' (lotto/small), 'medium' (high risk/medium), default 'large'.
- Use YYYY-MM-DD for dates. '0DTE' = today ({current_date_str}); parse phrases like 'this week' to next Friday.
- Output ONLY a valid JSON object with: 'classification', 'reason' (brief), 'confidence_score' (1-10).
- Add for 'new_trade': 'ticker', 'option_type', 'strike_price', 'expiration_date', 'sizing'.
- Add for 'trade_update'/'trade_close': 'related_trade_id', 'details' (e.g., 'trim 50%').

**Active Trades** (JSON for reference):
{active_trades_str}

**Reply Chain** (oldest to newest, for context):
{reply_chain_str}

**Message to Analyze**:
{message_content}
"""

            self.logger.info(f"LLM Prompt:\n{prompt}")

            response = self.grok_client.chat.completions.create(
                model="grok-3-mini",
                messages=[
                    {"role": "system", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0,  # Set to 0.0 for more consistent outputs
            )
            
            raw_response = response.choices[0].message.content
            usage = response.usage.dict()
            self.logger.info(f"LLM raw response: {raw_response}")
            result = json.loads(raw_response)
            
            if 'classification' not in result:
                raise ValueError("Missing required 'classification' field in analysis result")
            
            if 'expiration_date' in result and result['expiration_date']:
                result['expiration_date'] = parse_expiration_date(result['expiration_date'])

            # Return the parsed dict, usage stats, and the raw string for logging
            return result, usage, raw_response
            
        except Exception as e:
            self.logger.error(f"Analysis error: {str(e)}")
            error_resp = {
                'classification': 'other',
                'reason': f'Error: {str(e)}'
            }
            # Return None for raw_response on error
            return error_resp, {'prompt_tokens': 0, 'completion_tokens': 0, 'total_tokens': 0}, None

class MessageProcessor:
    def __init__(self):
        self.redis_client = redis_client
        self.db_client = db_client
        self.analyzer = GrokAnalyzer()
        self.max_reply_depth = 10
        self.history_limit = 10
        self.queue_name = MESSAGE_QUEUE

    def fetch_message_from_db(self, message_id: str) -> Optional[Dict]:
        query = "SELECT * FROM messages WHERE message_id = ?"
        return self.db_client.fetchone(query, (message_id,))

    def fetch_reply_chain(self, message_id: str, chain: List[str] = None, depth: int = 0) -> str:
        chain = chain or []
        if depth > self.max_reply_depth:
            return "Chain too deep."
        message = self.fetch_message_from_db(message_id)
        if not message:
            return ""
        chain.append(message["content"])
        if message["parent_id"]:
            self.fetch_reply_chain(str(message["parent_id"]), chain, depth + 1)
        return " > ".join(reversed(chain))

    def load_active_trades(self) -> List[Dict]:
        # Select only the columns needed for the LLM context to avoid schema errors
        query = """
            SELECT trade_id, symbol, option_type, strike, expiration, status, quantity
            FROM trades 
            WHERE status in ('open', 'pending_open') 
            ORDER BY created_at DESC
        """
        return self.db_client.fetchall(query)

    def load_recent_history(self, channel_id: int, timestamp: str, limit: int = None) -> List[Dict]:
        limit = limit or self.history_limit
        query = "SELECT content FROM messages WHERE channel_id = ? AND timestamp < ? ORDER BY timestamp DESC LIMIT ?"
        rows = self.db_client.fetchall(query, (channel_id, timestamp, limit))
        return [{"role": "user", "content": row["content"]} for row in reversed(rows)]

    def store_analysis(self, message_id: str, analysis: Dict, usage: Dict, raw_response: Optional[str] = None):
        """Stores the analysis result in the message_analyses table."""
        try:
            # Extract all key fields from the analysis
            classification = analysis.get("classification", "other")
            related_trade_id = analysis.get("related_trade_id")
            reason = analysis.get("reason")
            confidence_score = analysis.get("confidence_score")

            # Sanitize related_trade_id to ensure it's a valid foreign key or NULL
            if not related_trade_id or related_trade_id == 0:
                related_trade_id = None

            # The full, raw analysis is stored for complete auditability
            analysis_payload_json = json.dumps(analysis, default=str)

            query = """
                INSERT INTO message_analyses (
                    message_id, classification, related_trade_id, reason, 
                    confidence_score, analysis_payload
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(message_id) DO UPDATE SET
                    classification = excluded.classification,
                    related_trade_id = excluded.related_trade_id,
                    reason = excluded.reason,
                    confidence_score = excluded.confidence_score,
                    analysis_payload = excluded.analysis_payload;
            """
            params = (
                message_id, classification, related_trade_id, reason, 
                confidence_score, analysis_payload_json
            )
            self.db_client.execute(query, params)
            logger.info(f"Stored analysis for message {message_id} with classification: {classification}")

        except Exception as e:
            logger.error(f"Failed to store analysis for message {message_id}: {e}", exc_info=True)

    def queue_trade_action(self, message_id: str, analysis: Dict):
        action_data = {
            'message_id': message_id,
            'analysis': analysis
        }
        self.redis_client.push_to_queue(TRADE_ACTIONS_QUEUE, json.dumps(action_data))
        logger.info(f"Queued action for message {message_id}: {analysis.get('recommended_trade_action')}")

    def mark_message_processed(self, message_id: str):
        query = "UPDATE messages SET processed = TRUE WHERE message_id = ?"
        self.db_client.execute(query, (message_id,))
        logger.debug(f"Marked message {message_id} as processed")

    def process_message(self, message: dict):
        try:
            message_id = str(message['message_id'])
            logger.info(f"Processing {message_id}")
            
            existing = self.fetch_message_from_db(message_id)
            if not existing:
                logger.warning(f"Message {message_id} not found in DB")
                return
            
            content = message.get('content', '').strip()
            logger.info(f"Message content: {content}")
            
            if not content:
                logger.warning(f"No content in {message_id}")
                self.mark_message_processed(message_id)
                return
            
            # Check cache
            query = "SELECT analysis_payload FROM message_analyses WHERE message_id = ?"
            cached = self.db_client.fetchone(query, (message_id,))
            if cached:
                analysis = json.loads(cached['analysis_payload'])
                logger.info(f"Using cached analysis for {message_id}")
                logger.debug(f"Cached analysis details: {json.dumps(analysis, indent=2)}")
                logger.info(f"Cached classification: {analysis.get('classification')}")
            else:
                reply_chain = self.fetch_reply_chain(message_id)
                active_trades = self.load_active_trades()
                history = self.load_recent_history(message["channel_id"], message["timestamp"])
                analysis, usage, raw_response = self.analyzer.analyze_message(message['content'], history, reply_chain, active_trades)
                self.store_analysis(message_id, analysis, usage, raw_response)
            
            if analysis["classification"] in ("new_trade", "trade_update", "trade_close"):
                self.queue_trade_action(message_id, analysis)
            
            self.mark_message_processed(message_id)
            
        except Exception as e:
            logger.error(f"Process error: {str(e)}")

    def run(self):
        logger.info("Processor started")
        while True:
            try:
                message = self.redis_client.pop_from_queue(self.queue_name, timeout=1, is_json=True)
                if message:
                    self.process_message(message)
            except Exception as e:
                logger.error(f"Run error: {str(e)}")