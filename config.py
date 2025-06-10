import os
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
RAW_CHANNEL_MAP = os.getenv("DISCORD_CHANNEL_MAP", "")

if not DISCORD_TOKEN:
    raise ValueError("DISCORD_TOKEN is not set in the environment.")

if not RAW_CHANNEL_MAP:
    raise ValueError("DISCORD_CHANNEL_MAP is not set or empty.")

# Parse channel map: "123:Ashley,456:Bob" -> {"123": "Ashley", "456": "Bob"}
CHANNEL_TO_USER: dict[str, str] = {}
for pair in RAW_CHANNEL_MAP.split(","):
    try:
        channel_id, user = pair.strip().split(":")
        CHANNEL_TO_USER[channel_id.strip()] = user.strip()
    except ValueError:
        raise ValueError(f"Invalid format in DISCORD_CHANNEL_MAP: '{pair}' (expected format: id:username)")

# For convenience
CHANNEL_IDS: set[str] = set(CHANNEL_TO_USER.keys())
