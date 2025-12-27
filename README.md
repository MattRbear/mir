# mir - Cryptocurrency Trading System

`mir` is a modular cryptocurrency trading system that supports multiple trading strategies and real-time market data from public APIs.

## Features

- **Real-time Price Fetching**: Retrieves cryptocurrency prices from CoinGecko API
- **Portfolio Management**: Tracks cash balance, holdings, and transaction history
- **Trading Strategies**: Pluggable strategy system with built-in Moving Average Crossover strategy
- **Configuration-driven**: Easy setup via YAML configuration files
- **CLI Interface**: Simple command-line interface for running the trading system

## Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager

### Setup

1. Clone the repository:
```bash
git clone https://github.com/MattRbear/mir.git
cd mir
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install the package:
```bash
pip install -e .
```

## Configuration

Create or modify `config.yaml` to configure the trading system:

```yaml
# Portfolio settings
portfolio:
  initial_cash: 10000.0  # Starting cash balance in USD

# Data fetcher settings
data_fetcher:
  base_url: "https://api.coingecko.com/api/v3"

# Trading strategies
strategies:
  - type: moving_average
    symbol: bitcoin
    params:
      short_window: 5      # Short MA window
      long_window: 20      # Long MA window
      position_pct: 0.1    # 10% of cash per trade

# Engine settings
engine:
  iterations: 10  # Number of iterations (0 for infinite)
  interval: 60    # Seconds between iterations
```

## Usage

### Basic Usage

Run the trading system with default configuration:

```bash
mir
```

### Custom Configuration

Specify a custom configuration file:

```bash
mir --config my_config.yaml
```

### Command-line Options

```bash
mir --help
```

Available options:
- `--config PATH`: Path to configuration file (default: config.yaml)
- `--dry-run`: Run in simulation mode
- `--iterations N`: Number of trading iterations (overrides config)
- `--interval N`: Seconds between iterations (overrides config)

### Examples

Run 5 iterations with 30-second intervals:
```bash
mir --iterations 5 --interval 30
```

Run in dry-run mode with custom config:
```bash
mir --config test_config.yaml --dry-run
```

## Architecture

### Components

1. **DataFetcher** (`mir/data_fetcher.py`): Fetches real-time cryptocurrency prices from APIs
2. **Portfolio** (`mir/portfolio.py`): Manages cash balance, holdings, and executes trades
3. **TradingStrategy** (`mir/strategies/`): Base class and implementations for trading strategies
4. **TradingEngine** (`mir/engine.py`): Orchestrates the trading loop and coordinates components
5. **Config** (`mir/config.py`): Manages configuration loading and defaults

### Trading Strategies

#### Moving Average Crossover

The built-in Moving Average strategy generates:
- **Buy Signal**: When short-term MA crosses above long-term MA
- **Sell Signal**: When short-term MA crosses below long-term MA

Position sizing is controlled by the `position_pct` parameter.

## Development

### Project Structure

```
mir/
├── mir/                    # Main package
│   ├── __init__.py
│   ├── main.py            # Entry point
│   ├── config.py          # Configuration management
│   ├── data_fetcher.py    # Price data fetching
│   ├── portfolio.py       # Portfolio management
│   ├── engine.py          # Trading engine
│   └── strategies/        # Trading strategies
│       ├── __init__.py
│       └── moving_average.py
├── tests/                 # Test suite
├── config.yaml           # Default configuration
├── requirements.txt      # Python dependencies
├── setup.py             # Package setup
└── README.md            # This file
```

### Adding New Strategies

To create a custom trading strategy:

1. Create a new file in `mir/strategies/`
2. Inherit from `TradingStrategy` base class
3. Implement required methods: `should_buy()`, `should_sell()`, `get_position_size()`
4. Update `mir/main.py` to register your strategy type

Example:
```python
from mir.strategies import TradingStrategy

class MyStrategy(TradingStrategy):
    def should_buy(self) -> bool:
        # Your buy logic
        pass
    
    def should_sell(self) -> bool:
        # Your sell logic
        pass
    
    def get_position_size(self, available_cash, current_price) -> float:
        # Your position sizing logic
        pass
```

## API Rate Limits

The system uses the free CoinGecko API which has rate limits. For production use, consider:
- Implementing rate limiting in the data fetcher
- Using a paid API tier
- Caching price data appropriately

## Disclaimer

**IMPORTANT**: This trading system is for educational and research purposes only. It does not execute real trades on exchanges. Always:
- Test thoroughly before any real trading
- Understand the risks of cryptocurrency trading
- Never trade with money you cannot afford to lose
- Comply with all applicable laws and regulations

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
