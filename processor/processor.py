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
from commons.mongodb.mongodb_client import MongoDBClient
from utils.logger import get_logger

# Set up logging
logger = get_logger(__name__)

load_dotenv()
MESSAGE_QUEUE = os.getenv('MESSAGE_QUEUE_NAME', 'message_queue')
TRADE_ACTIONS_QUEUE = os.getenv('TRADE_ACTIONS_QUEUE', 'trade_actions')
XAI_API_KEY = os.getenv('XAI_API_KEY')
MESSAGES_COLLECTION = os.getenv("MESSAGES_COLLECTION", "raw_messages")
ANALYZED_ACTIONS_COLLECTION = os.getenv("ANALYZED_ACTIONS_COLLECTION", "analyzed_actions")
TRADES_COLLECTION = os.getenv("TRADES_COLLECTION", "trades")

mongo_client = MongoDBClient()
messages_coll = mongo_client.get_collection(MESSAGES_COLLECTION)
analyses_coll = mongo_client.get_collection(ANALYZED_ACTIONS_COLLECTION)
trades_coll = mongo_client.get_collection(TRADES_COLLECTION)

def parse_expiration_date(date_str: str) -> str:
    """Parse various string representations of expiration dates into YYYY-MM-DD format."""
    if not isinstance(date_str, str):
        return date.today().isoformat()  # Default to today if invalid input

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
        self.logger = get_logger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.grok_client = OpenAI(api_key=XAI_API_KEY, base_url="https://api.x.ai/v1") if XAI_API_KEY else None
        if not self.grok_client:
            raise ValueError("XAI_API_KEY missing")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def analyze_message(self, content: str, history: List[Dict], reply_chain: str, todays_trades: List[Dict]) -> Tuple[Dict, Dict, Optional[str]]:
        """Analyze a message using the Grok API and return the structured analysis."""
        if not self.grok_client:
            return {'action_type': 'other', 'reason': 'Grok client not initialized'}, {}, None

        self.logger.debug(f"Analyzing message content: {content}")
        try:
            # Prepare context for the prompt
            todays_trades_str = json.dumps(todays_trades, indent=2, default=str)
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
- Classify as 'new_trade' if message has explicit details: ticker, CALL/PUT, strike_price. Infer expiration_date if not specified: default to today ({current_date_str}) if today is Friday (0DTE), otherwise to next Friday ('this week').
- For 'trade_update' or 'trade_close', MUST reference an active trade by symbol or details; include 'related_trade_id' (exact integer from active trades). If no match, classify as 'irrelevant'. Example: If message says "trim ORCL" and active trades have {{"trade_id": 456, "symbol": "ORCL"}}, use 456.
- Infer 'sizing' for new trades: 'small' (lotto/small), 'medium' (high risk/medium), default 'large'.
- Use YYYY-MM-DD for dates. '0DTE' = today ({current_date_str}); parse phrases like 'this week' to next Friday.
- Output ONLY a valid JSON object with: 'action_type', 'reason' (brief), 'confidence_score' (1-10).
- Add for 'new_trade': 'ticker', 'option_type', 'strike_price', 'expiration_date', 'sizing'.
- Add for 'trade_update'/'trade_close': 'related_trade_id', 'details' (e.g., 'trim 50%').

**Today's Trades** (JSON for reference, including all statuses for context):
{todays_trades_str}

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
                temperature=0.2,  # Set to 0.0 for more consistent outputs
            )
            
            raw_response = response.choices[0].message.content
            usage = response.usage.model_dump()
            self.logger.info(f"LLM raw response: {raw_response}")
            result = json.loads(raw_response)
            
            if 'action_type' not in result:
                raise ValueError("Missing required 'action_type' field in analysis result")
            
            if 'expiration_date' in result and result['expiration_date']:
                result['expiration_date'] = parse_expiration_date(result['expiration_date'])

            # Return the parsed dict, usage stats, and the raw string for logging
            return result, usage, raw_response
            
        except Exception as e:
            self.logger.error(f"Analysis error: {str(e)}")
            error_resp = {
                'action_type': 'other',
                'reason': f'Error: {str(e)}'
            }
            # Return None for raw_response on error
            return error_resp, {'prompt_tokens': 0, 'completion_tokens': 0, 'total_tokens': 0}, None

class MessageProcessor:
    def __init__(self):
        self.redis_client = redis_client
        self.analyzer = GrokAnalyzer()
        self.max_reply_depth = 10
        self.history_limit = 10
        self.queue_name = MESSAGE_QUEUE

    def fetch_message_from_db(self, message_id: str) -> Optional[Dict]:
        return messages_coll.find_one({"message_id": int(message_id)})

    def fetch_reply_chain(self, message_id: str) -> str:
        """Fetch the full conversation thread for context."""
        try:
            message_id = int(message_id)
            # Find the root by traversing upwards
            current_id = message_id
            parent_chain_ids = []
            visited = set()
            while current_id and current_id not in visited:
                visited.add(current_id)
                parent_chain_ids.append(current_id)
                msg = messages_coll.find_one({"message_id": current_id})
                if not msg:
                    break
                current_id = msg.get("parent_id")

            if not parent_chain_ids:
                return ""

            root_id = parent_chain_ids[-1]  # Last ID in the chain is the root

            # Now traverse the entire thread from the root downwards
            chain = []
            visited = set()

            def add_message(msg_id, level=0):
                if msg_id in visited:
                    return
                visited.add(msg_id)
                msg = messages_coll.find_one({"message_id": msg_id})
                if not msg:
                    return
                chain.append({
                    "content": msg["content"],
                    "timestamp": msg["timestamp"],
                    "level": level
                })
                # Fetch and add replies, sorted by timestamp
                replies = list(messages_coll.find({"parent_id": msg_id}).sort("timestamp", 1))
                for reply in replies:
                    add_message(reply["message_id"], level + 1)

            # Start from root
            add_message(root_id)

            # Sort chronologically and format
            chain.sort(key=lambda x: x.get("timestamp", ""))
            formatted_chain = "\n".join([f"{'  ' * item['level']}{item['content']}" for item in chain])
            return formatted_chain
        except Exception as e:
            logger.error(f"Error fetching reply chain: {e}")
            return ""

    def load_todays_trades(self) -> List[Dict]:
        today_start = datetime.combine(date.today(), datetime.min.time()).isoformat()
        query = {"created_at": {"$gte": today_start}}
        projection = {
            "trade_id": 1, "symbol": 1, "option_type": 1, "strike": 1, 
            "expiration": 1, "status": 1, "quantity": 1
        }
        return list(trades_coll.find(query, projection).sort("created_at", -1))

    def load_recent_history(self, channel_name: str, timestamp: str, limit: int = None) -> List[Dict]:
        limit = limit or self.history_limit
        query = {"channel_name": channel_name, "timestamp": {"$lt": timestamp}}
        projection = {"content": 1}
        rows = list(messages_coll.find(query, projection).sort("timestamp", -1).limit(limit))
        return [{"role": "user", "content": row["content"]} for row in reversed(rows)]

    def store_analysis(self, message_id: str, analysis: Dict, usage: Dict, raw_response: Optional[str] = None):
        """Stores the analysis result in the analyses collection."""
        try:
            # Extract key fields
            action_type = analysis.get("action_type", "other")
            related_trade_id = analysis.get("related_trade_id")
            reason = analysis.get("reason")
            confidence_score = analysis.get("confidence_score")

            # Sanitize related_trade_id
            if not related_trade_id or related_trade_id == 0:
                related_trade_id = None

            # Full payload
            analysis_payload = analysis
            analysis_payload["usage"] = usage
            if raw_response:
                analysis_payload["raw_response"] = raw_response

            doc = {
                "message_id": int(message_id),
                "action_type": action_type,
                "related_trade_id": related_trade_id,
                "reason": reason,
                "confidence_score": confidence_score,
                "analysis_payload": analysis_payload,
                "analysis_timestamp": datetime.now().isoformat()
            }
            analyses_coll.update_one(
                {"message_id": int(message_id)},
                {"$set": doc},
                upsert=True
            )
            logger.info(f"Stored analysis for message {message_id} with action_type: {action_type}")

        except Exception as e:
            logger.error(f"Failed to store analysis for message {message_id}: {e}", exc_info=True)

    def queue_trade_action(self, message_id: str, analysis: Dict):
        action_data = {
            'message_id': message_id,
            'analysis': analysis
        }
        self.redis_client.push_to_queue(TRADE_ACTIONS_QUEUE, json.dumps(action_data))
        logger.info(f"Queued action for message {message_id}: {analysis.get('action_type')}")

    def mark_message_processed(self, message_id: str):
        messages_coll.update_one(
            {"message_id": int(message_id)},
            {"$set": {"processed": True}}
        )
        logger.debug(f"Marked message {message_id} as processed")

    def process_message(self, message: dict):
        try:
            message_id = str(message['message_id'])
            logger.info(f"Processing {message_id}")
            
            existing = self.fetch_message_from_db(message_id)
            if not existing:
                logger.warning(f"Message {message_id} not found in DB")
                return
            
            content = existing.get('content', '').strip()
            logger.info(f"Message content: {content}")
            
            if not content:
                logger.warning(f"No content in {message_id}")
                self.mark_message_processed(message_id)
                return
            
            # Check cache
            cached = analyses_coll.find_one({"message_id": int(message_id)})
            if cached:
                analysis = cached.get("analysis_payload", {})
                logger.info(f"Using cached analysis for {message_id}")
                logger.debug(f"Cached analysis details: {json.dumps(analysis, indent=2)}")
                logger.info(f"Cached action_type: {analysis.get('action_type')}")
            else:
                reply_chain = self.fetch_reply_chain(message_id)
                todays_trades = self.load_todays_trades()
                history = self.load_recent_history(existing.get("channel_name"), existing["timestamp"])
                analysis, usage, raw_response = self.analyzer.analyze_message(content, history, reply_chain, todays_trades)
                self.store_analysis(message_id, analysis, usage, raw_response)
            
            if analysis["action_type"] in ("new_trade", "trade_update", "trade_close"):
                self.queue_trade_action(message_id, analysis)
            
            self.mark_message_processed(message_id)
            
        except Exception as e:
            logger.error(f"Process error: {str(e)}")

    def run(self):
        logger.info("Processor started")
        try:
            while True:
                try:
                    message_str = self.redis_client.pop_from_queue(self.queue_name, timeout=1)
                    if message_str:
                        data = {'message_id': message_str}
                        self.process_message(data)
                except Exception as e:
                    logger.error(f"Run error: {str(e)}")
                    time.sleep(1)  # Prevent tight loop on errors
        except KeyboardInterrupt:
            logger.info("Processor shutting down gracefully")

if __name__ == "__main__":
    processor = MessageProcessor()
    processor.run()