# Quick Start Guide

This guide will help you get started with the mir cryptocurrency trading system.

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/MattRbear/mir.git
   cd mir
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Install the package:**
   ```bash
   pip install -e .
   ```

## Running Your First Trade

### Option 1: Using the CLI

Run the trading system with default settings:
```bash
mir
```

This will:
- Start with $10,000 in cash
- Trade Bitcoin using a Moving Average Crossover strategy
- Run for 10 iterations with 60 seconds between each

### Option 2: Custom Configuration

Create a custom configuration file `my_config.yaml`:
```yaml
portfolio:
  initial_cash: 20000.0

strategies:
  - type: moving_average
    symbol: bitcoin
    params:
      short_window: 5
      long_window: 20
      position_pct: 0.15
  
  - type: moving_average
    symbol: ethereum
    params:
      short_window: 7
      long_window: 25
      position_pct: 0.10

engine:
  iterations: 20
  interval: 30
```

Run with your custom configuration:
```bash
mir --config my_config.yaml
```

### Option 3: Programmatic Usage

Create a Python script:
```python
from mir.portfolio import Portfolio
from mir.data_fetcher import DataFetcher
from mir.engine import TradingEngine
from mir.strategies.moving_average import MovingAverageStrategy

# Initialize components
portfolio = Portfolio(initial_cash=10000.0)
data_fetcher = DataFetcher()
engine = TradingEngine(portfolio, data_fetcher)

# Add strategy
strategy = MovingAverageStrategy(
    symbol="bitcoin",
    short_window=5,
    long_window=20,
    position_pct=0.1
)
engine.add_strategy(strategy)

# Run the engine
engine.run(iterations=10, interval=60)
```

See `example.py` for a complete example.

## Understanding the Output

When running, you'll see output like:
```
--- Iteration 1 ---
Buy signal: Bullish crossover - Short MA (50123.45) crossed above Long MA (49876.54)
Bought 0.00199203 bitcoin at 50123.45 (Total: 1000.00)

=== Portfolio Summary ===
Cash: $9000.00

Holdings:
  bitcoin: 0.001992 (Value: $1000.00)

Total Portfolio Value: $10000.00
========================
```

## Common Commands

**Run in dry-run mode (simulation):**
```bash
mir --dry-run
```

**Run with specific iterations:**
```bash
mir --iterations 5
```

**Run with custom interval (seconds):**
```bash
mir --interval 30
```

**Combine options:**
```bash
mir --iterations 10 --interval 30 --dry-run
```

## Next Steps

- Read the full [README.md](README.md) for detailed documentation
- Explore the [example.py](example.py) script
- Modify `config.yaml` to suit your trading preferences
- Create your own trading strategies by extending the `TradingStrategy` class

## Important Notes

⚠️ **This system does not execute real trades on exchanges.** It's for educational and research purposes.

⚠️ **API Rate Limits:** The system uses the free CoinGecko API which has rate limits. Be mindful of your request frequency.

⚠️ **Internet Required:** You need an active internet connection to fetch real-time cryptocurrency prices.
