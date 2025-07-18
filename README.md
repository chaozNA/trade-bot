# Trade Bot

Modular trading bot with Discord listener, Redis queues, Grok analysis, SQLite DB, Alpaca execution, position monitoring.

## Setup
- Copy .env.example to .env, fill values.
- pip install -r requirements.txt

## Run
- python listener/collector.py  # Discord listener
- python processor/processor.py  # Analysis & queue to actions
- python manager/trade_manager.py  # Execute on Alpaca, monitor
- streamlit run ui/ui.py  # View UI

## Simulation
- Collector populates messages DB.
- Processor main processes sample IDs, queues actions.
- Trade manager processes queue, "executes" (paper).
- Uses today for dates; simulates as if messages now.