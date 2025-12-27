#!/usr/bin/env python3
"""
Example script demonstrating programmatic usage of the mir trading system
"""
from mir.data_fetcher import DataFetcher
from mir.portfolio import Portfolio
from mir.engine import TradingEngine
from mir.strategies.moving_average import MovingAverageStrategy


def main():
    """Main example function"""
    print("=" * 60)
    print("mir Crypto Trading System - Example Usage")
    print("=" * 60)
    print()
    
    # Initialize portfolio with $10,000
    portfolio = Portfolio(initial_cash=10000.0)
    print(f"Initial cash: ${portfolio.get_cash():.2f}")
    print()
    
    # Initialize data fetcher
    data_fetcher = DataFetcher()
    
    # Create trading engine
    engine = TradingEngine(portfolio, data_fetcher)
    
    # Add a Moving Average strategy for Bitcoin
    btc_strategy = MovingAverageStrategy(
        symbol="bitcoin",
        short_window=5,
        long_window=20,
        position_pct=0.1  # Use 10% of available cash per trade
    )
    engine.add_strategy(btc_strategy)
    
    # Add a Moving Average strategy for Ethereum
    eth_strategy = MovingAverageStrategy(
        symbol="ethereum",
        short_window=7,
        long_window=25,
        position_pct=0.1
    )
    engine.add_strategy(eth_strategy)
    
    print("\nStrategies configured:")
    print("- Bitcoin: MA(5, 20) with 10% position sizing")
    print("- Ethereum: MA(7, 25) with 10% position sizing")
    print()
    
    # Run the trading engine
    print("Starting trading engine...")
    print("Note: This requires internet connection to fetch prices")
    print()
    
    try:
        # Run for 3 iterations with 30 seconds between each
        engine.run(iterations=3, interval=30)
    except Exception as e:
        print(f"\nError during execution: {e}")
        print("This is expected if running without internet access")
    
    print("\n" + "=" * 60)
    print("Example completed")
    print("=" * 60)


if __name__ == "__main__":
    main()
