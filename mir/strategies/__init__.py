"""
Base trading strategy interface
"""
from abc import ABC, abstractmethod
from typing import Optional, Tuple


class TradingStrategy(ABC):
    """Abstract base class for trading strategies"""
    
    def __init__(self, symbol: str):
        """
        Initialize strategy
        
        Args:
            symbol: Cryptocurrency symbol to trade
        """
        self.symbol = symbol
        self.price_history = []
    
    def update_price(self, price: float):
        """
        Update price history
        
        Args:
            price: Current price
        """
        self.price_history.append(price)
    
    @abstractmethod
    def should_buy(self) -> bool:
        """
        Determine if strategy indicates a buy signal
        
        Returns:
            True if should buy, False otherwise
        """
        pass
    
    @abstractmethod
    def should_sell(self) -> bool:
        """
        Determine if strategy indicates a sell signal
        
        Returns:
            True if should sell, False otherwise
        """
        pass
    
    @abstractmethod
    def get_position_size(self, available_cash: float, current_price: float) -> float:
        """
        Calculate position size for a trade
        
        Args:
            available_cash: Available cash for trading
            current_price: Current price of the asset
            
        Returns:
            Amount to trade
        """
        pass
