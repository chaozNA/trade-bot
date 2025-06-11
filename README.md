# Trade Bot

A modular, extensible trading bot with Discord integration and support for future trading API integrations (e.g., Alpaca).

## Project Structure

```
trade-bot/
│
├── bot/               # Core bot logic
├── config/            # Configuration files
├── integrations/      # Third-party API integrations
├── data/              # Data storage
├── logs/              # Log files (future use)
├── tests/             # Unit tests (future use)
├── requirements.txt   # Python dependencies
├── .env               # Environment variables (not committed)
├── .env.example       # Example env file
├── README.md          # This file
```

## Setup

1. Copy `.env.example` to `.env` and fill in your credentials.
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
3. Run the bot:
   ```sh
   python -m bot.main
   ```

## Environment Variables
See `.env.example` for required variables.

## Logging
Log files will be stored in the `logs/` directory (future).

## Testing
Tests will go in the `tests/` directory (future).

---

## Contributing
- Add new integrations in `integrations/`.
- Improve logging in `bot/` or add handlers in `logs/`.
- Keep configuration in `config/`.
