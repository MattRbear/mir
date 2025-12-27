"""
Portfolio management module
"""
from typing import Dict, Optional


class Portfolio:
    """Manages cryptocurrency holdings and cash balance"""
    
    def __init__(self, initial_cash: float = 10000.0):
        """
        Initialize portfolio
        
        Args:
            initial_cash: Starting cash balance
        """
        self.cash = initial_cash
        self.holdings: Dict[str, float] = {}
        self.transaction_history = []
        
    def get_cash(self) -> float:
        """Get current cash balance"""
        return self.cash
    
    def get_holdings(self) -> Dict[str, float]:
        """Get current cryptocurrency holdings"""
        return self.holdings.copy()
    
    def get_position(self, symbol: str) -> float:
        """
        Get position size for a symbol
        
        Args:
            symbol: Cryptocurrency symbol
            
        Returns:
            Amount held (0 if not held)
        """
        return self.holdings.get(symbol, 0.0)
    
    def buy(self, symbol: str, amount: float, price: float) -> bool:
        """
        Execute a buy order
        
        Args:
            symbol: Cryptocurrency symbol
            amount: Amount to buy
            price: Price per unit
            
        Returns:
            True if successful, False otherwise
        """
        cost = amount * price
        if cost > self.cash:
            print(f"Insufficient funds to buy {amount} {symbol} at {price}")
            return False
        
        self.cash -= cost
        self.holdings[symbol] = self.holdings.get(symbol, 0.0) + amount
        
        transaction = {
            "type": "buy",
            "symbol": symbol,
            "amount": amount,
            "price": price,
            "total": cost
        }
        self.transaction_history.append(transaction)
        print(f"Bought {amount} {symbol} at {price} (Total: {cost})")
        return True
    
    def sell(self, symbol: str, amount: float, price: float) -> bool:
        """
        Execute a sell order
        
        Args:
            symbol: Cryptocurrency symbol
            amount: Amount to sell
            price: Price per unit
            
        Returns:
            True if successful, False otherwise
        """
        current_holdings = self.holdings.get(symbol, 0.0)
        if amount > current_holdings:
            print(f"Insufficient holdings to sell {amount} {symbol}")
            return False
        
        proceeds = amount * price
        self.cash += proceeds
        self.holdings[symbol] = current_holdings - amount
        
        if self.holdings[symbol] == 0:
            del self.holdings[symbol]
        
        transaction = {
            "type": "sell",
            "symbol": symbol,
            "amount": amount,
            "price": price,
            "total": proceeds
        }
        self.transaction_history.append(transaction)
        print(f"Sold {amount} {symbol} at {price} (Total: {proceeds})")
        return True
    
    def get_portfolio_value(self, prices: Dict[str, float]) -> float:
        """
        Calculate total portfolio value
        
        Args:
            prices: Dictionary of current prices for held symbols
            
        Returns:
            Total portfolio value including cash
        """
        holdings_value = sum(
            amount * prices.get(symbol, 0.0)
            for symbol, amount in self.holdings.items()
        )
        return self.cash + holdings_value
    
    def print_summary(self, prices: Optional[Dict[str, float]] = None):
        """Print portfolio summary"""
        print("\n=== Portfolio Summary ===")
        print(f"Cash: ${self.cash:.2f}")
        print("\nHoldings:")
        for symbol, amount in self.holdings.items():
            if prices and symbol in prices:
                value = amount * prices[symbol]
                print(f"  {symbol}: {amount:.6f} (Value: ${value:.2f})")
            else:
                print(f"  {symbol}: {amount:.6f}")
        
        if prices:
            total_value = self.get_portfolio_value(prices)
            print(f"\nTotal Portfolio Value: ${total_value:.2f}")
        print("========================\n")
