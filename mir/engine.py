"""
Trading engine that orchestrates the trading system
"""
from typing import List, Dict
from mir.data_fetcher import DataFetcher
from mir.portfolio import Portfolio
from mir.strategies import TradingStrategy
import time


class TradingEngine:
    """Main trading engine that coordinates strategies and execution"""
    
    def __init__(self, portfolio: Portfolio, data_fetcher: DataFetcher):
        """
        Initialize trading engine
        
        Args:
            portfolio: Portfolio instance
            data_fetcher: Data fetcher instance
        """
        self.portfolio = portfolio
        self.data_fetcher = data_fetcher
        self.strategies: Dict[str, TradingStrategy] = {}
        
    def add_strategy(self, strategy: TradingStrategy):
        """
        Add a trading strategy
        
        Args:
            strategy: Trading strategy instance
        """
        self.strategies[strategy.symbol] = strategy
        print(f"Added strategy for {strategy.symbol}")
    
    def run_iteration(self):
        """Run one iteration of the trading engine"""
        if not self.strategies:
            print("No strategies configured")
            return
        
        # Get current prices for all symbols
        symbols = list(self.strategies.keys())
        prices = self.data_fetcher.get_multiple_prices(symbols)
        
        # Update strategies with new prices and check for signals
        for symbol, strategy in self.strategies.items():
            price = prices.get(symbol)
            if price is None:
                print(f"Could not fetch price for {symbol}")
                continue
            
            # Update strategy with new price
            strategy.update_price(price)
            
            # Check for buy signal
            if strategy.should_buy():
                position_size = strategy.get_position_size(
                    self.portfolio.get_cash(), 
                    price
                )
                if position_size > 0:
                    self.portfolio.buy(symbol, position_size, price)
            
            # Check for sell signal
            elif strategy.should_sell():
                current_position = self.portfolio.get_position(symbol)
                if current_position > 0:
                    self.portfolio.sell(symbol, current_position, price)
        
        # Print portfolio summary
        self.portfolio.print_summary(prices)
    
    def run(self, iterations: int = 10, interval: int = 60):
        """
        Run the trading engine for multiple iterations
        
        Args:
            iterations: Number of iterations to run (0 for infinite)
            interval: Seconds between iterations
        """
        print(f"\n{'='*50}")
        print("Starting Trading Engine")
        print(f"Iterations: {'Infinite' if iterations == 0 else iterations}")
        print(f"Interval: {interval} seconds")
        print(f"{'='*50}\n")
        
        iteration_count = 0
        try:
            while iterations == 0 or iteration_count < iterations:
                iteration_count += 1
                print(f"\n--- Iteration {iteration_count} ---")
                self.run_iteration()
                
                if iterations == 0 or iteration_count < iterations:
                    print(f"Waiting {interval} seconds until next iteration...")
                    time.sleep(interval)
                    
        except KeyboardInterrupt:
            print("\n\nTrading engine stopped by user")
        
        print(f"\n{'='*50}")
        print("Trading Engine Stopped")
        print(f"Total iterations: {iteration_count}")
        print(f"{'='*50}\n")
