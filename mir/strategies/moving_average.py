"""
Moving Average Crossover Strategy
"""
from mir.strategies import TradingStrategy


class MovingAverageStrategy(TradingStrategy):
    """
    Simple Moving Average Crossover Strategy
    
    Generates buy signal when short MA crosses above long MA
    Generates sell signal when short MA crosses below long MA
    """
    
    def __init__(self, symbol: str, short_window: int = 5, long_window: int = 20, position_pct: float = 0.1):
        """
        Initialize Moving Average Strategy
        
        Args:
            symbol: Cryptocurrency symbol to trade
            short_window: Short moving average window
            long_window: Long moving average window
            position_pct: Percentage of available funds to use per trade (0.0 to 1.0)
        """
        super().__init__(symbol)
        self.short_window = short_window
        self.long_window = long_window
        self.position_pct = position_pct
        self.last_signal = None
        
    def _calculate_ma(self, window: int) -> float:
        """Calculate moving average for given window"""
        if len(self.price_history) < window:
            return None
        return sum(self.price_history[-window:]) / window
    
    def should_buy(self) -> bool:
        """Buy when short MA crosses above long MA"""
        if len(self.price_history) < self.long_window:
            return False
        
        short_ma = self._calculate_ma(self.short_window)
        long_ma = self._calculate_ma(self.long_window)
        
        if short_ma is None or long_ma is None:
            return False
        
        # Check for bullish crossover
        if short_ma > long_ma:
            if self.last_signal != "buy":
                self.last_signal = "buy"
                print(f"Buy signal: Short MA ({short_ma:.2f}) > Long MA ({long_ma:.2f})")
                return True
        
        return False
    
    def should_sell(self) -> bool:
        """Sell when short MA crosses below long MA"""
        if len(self.price_history) < self.long_window:
            return False
        
        short_ma = self._calculate_ma(self.short_window)
        long_ma = self._calculate_ma(self.long_window)
        
        if short_ma is None or long_ma is None:
            return False
        
        # Check for bearish crossover
        if short_ma < long_ma:
            if self.last_signal != "sell":
                self.last_signal = "sell"
                print(f"Sell signal: Short MA ({short_ma:.2f}) < Long MA ({long_ma:.2f})")
                return True
        
        return False
    
    def get_position_size(self, available_cash: float, current_price: float) -> float:
        """
        Calculate position size based on available cash and position percentage
        
        Args:
            available_cash: Available cash for trading
            current_price: Current price of the asset
            
        Returns:
            Amount to trade
        """
        if current_price <= 0:
            return 0.0
        
        trade_cash = available_cash * self.position_pct
        return trade_cash / current_price
