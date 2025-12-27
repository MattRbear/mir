"""
Main entry point for the mir trading system
"""
import argparse
import sys
from mir.config import Config
from mir.data_fetcher import DataFetcher
from mir.portfolio import Portfolio
from mir.engine import TradingEngine
from mir.strategies.moving_average import MovingAverageStrategy


def create_strategy(strategy_config: dict):
    """
    Create a trading strategy from configuration
    
    Args:
        strategy_config: Strategy configuration dictionary
        
    Returns:
        TradingStrategy instance
    """
    strategy_type = strategy_config.get("type", "moving_average")
    symbol = strategy_config.get("symbol", "bitcoin")
    params = strategy_config.get("params", {})
    
    if strategy_type == "moving_average":
        return MovingAverageStrategy(
            symbol=symbol,
            short_window=params.get("short_window", 5),
            long_window=params.get("long_window", 20),
            position_pct=params.get("position_pct", 0.1)
        )
    else:
        raise ValueError(f"Unknown strategy type: {strategy_type}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="mir - Cryptocurrency Trading System"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (simulation only)"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        help="Number of trading iterations (overrides config)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        help="Seconds between iterations (overrides config)"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = Config.from_file(args.config)
    
    # Initialize components
    portfolio = Portfolio(
        initial_cash=config.get("portfolio.initial_cash", 10000.0)
    )
    
    data_fetcher = DataFetcher(
        base_url=config.get("data_fetcher.base_url", "https://api.coingecko.com/api/v3")
    )
    
    engine = TradingEngine(portfolio, data_fetcher)
    
    # Add strategies
    strategies_config = config.get("strategies", [])
    if not strategies_config:
        print("Warning: No strategies configured, using default")
        strategies_config = [
            {
                "type": "moving_average",
                "symbol": "bitcoin",
                "params": {
                    "short_window": 5,
                    "long_window": 20,
                    "position_pct": 0.1
                }
            }
        ]
    
    for strategy_config in strategies_config:
        try:
            strategy = create_strategy(strategy_config)
            engine.add_strategy(strategy)
        except Exception as e:
            print(f"Error creating strategy: {e}")
            sys.exit(1)
    
    # Get engine parameters
    iterations = args.iterations if args.iterations is not None else config.get("engine.iterations", 10)
    interval = args.interval if args.interval is not None else config.get("engine.interval", 60)
    
    # Run the engine
    if args.dry_run:
        print("\n*** DRY RUN MODE - Simulated trading only ***\n")
    
    try:
        engine.run(iterations=iterations, interval=interval)
    except Exception as e:
        print(f"Error running trading engine: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
