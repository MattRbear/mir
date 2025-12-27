"""
Data fetcher module for retrieving cryptocurrency prices
"""
import requests
from typing import Dict, Optional
import time


class DataFetcher:
    """Fetches cryptocurrency price data from public APIs"""
    
    def __init__(self, base_url: str = "https://api.coingecko.com/api/v3"):
        """
        Initialize the data fetcher
        
        Args:
            base_url: Base URL for the cryptocurrency API
        """
        self.base_url = base_url
        
    def get_price(self, symbol: str, vs_currency: str = "usd") -> Optional[float]:
        """
        Get the current price of a cryptocurrency
        
        Args:
            symbol: Cryptocurrency symbol (e.g., 'bitcoin', 'ethereum')
            vs_currency: Currency to get price in (default: 'usd')
            
        Returns:
            Current price as float, or None if request fails
        """
        try:
            url = f"{self.base_url}/simple/price"
            params = {
                "ids": symbol.lower(),
                "vs_currencies": vs_currency.lower()
            }
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if symbol.lower() in data and vs_currency.lower() in data[symbol.lower()]:
                return float(data[symbol.lower()][vs_currency.lower()])
            return None
        except Exception as e:
            print(f"Error fetching price for {symbol}: {e}")
            return None
    
    def get_multiple_prices(self, symbols: list, vs_currency: str = "usd") -> Dict[str, Optional[float]]:
        """
        Get current prices for multiple cryptocurrencies
        
        Args:
            symbols: List of cryptocurrency symbols
            vs_currency: Currency to get prices in (default: 'usd')
            
        Returns:
            Dictionary mapping symbols to prices
        """
        try:
            url = f"{self.base_url}/simple/price"
            ids = ",".join([s.lower() for s in symbols])
            params = {
                "ids": ids,
                "vs_currencies": vs_currency.lower()
            }
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            result = {}
            for symbol in symbols:
                symbol_lower = symbol.lower()
                if symbol_lower in data and vs_currency.lower() in data[symbol_lower]:
                    result[symbol] = float(data[symbol_lower][vs_currency.lower()])
                else:
                    result[symbol] = None
            return result
        except Exception as e:
            print(f"Error fetching multiple prices: {e}")
            return {symbol: None for symbol in symbols}
