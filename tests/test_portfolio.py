"""
Tests for Portfolio module
"""
import unittest
from mir.portfolio import Portfolio


class TestPortfolio(unittest.TestCase):
    """Test cases for Portfolio class"""
    
    def setUp(self):
        """Set up test portfolio"""
        self.portfolio = Portfolio(initial_cash=10000.0)
    
    def test_initial_cash(self):
        """Test initial cash balance"""
        self.assertEqual(self.portfolio.get_cash(), 10000.0)
    
    def test_initial_holdings_empty(self):
        """Test initial holdings are empty"""
        self.assertEqual(self.portfolio.get_holdings(), {})
    
    def test_buy_success(self):
        """Test successful buy operation"""
        result = self.portfolio.buy("bitcoin", 0.1, 5000.0)
        self.assertTrue(result)
        self.assertEqual(self.portfolio.get_position("bitcoin"), 0.1)
        self.assertEqual(self.portfolio.get_cash(), 10000.0 - 500.0)
    
    def test_buy_insufficient_funds(self):
        """Test buy with insufficient funds"""
        result = self.portfolio.buy("bitcoin", 1.0, 50000.0)
        self.assertFalse(result)
        self.assertEqual(self.portfolio.get_position("bitcoin"), 0.0)
        self.assertEqual(self.portfolio.get_cash(), 10000.0)
    
    def test_sell_success(self):
        """Test successful sell operation"""
        self.portfolio.buy("bitcoin", 1.0, 5000.0)
        result = self.portfolio.sell("bitcoin", 0.5, 5500.0)
        self.assertTrue(result)
        self.assertEqual(self.portfolio.get_position("bitcoin"), 0.5)
        expected_cash = 10000.0 - 5000.0 + (0.5 * 5500.0)
        self.assertEqual(self.portfolio.get_cash(), expected_cash)
    
    def test_sell_insufficient_holdings(self):
        """Test sell with insufficient holdings"""
        result = self.portfolio.sell("bitcoin", 1.0, 50000.0)
        self.assertFalse(result)
    
    def test_sell_removes_zero_holdings(self):
        """Test that selling all removes symbol from holdings"""
        self.portfolio.buy("bitcoin", 1.0, 5000.0)
        self.portfolio.sell("bitcoin", 1.0, 5500.0)
        self.assertNotIn("bitcoin", self.portfolio.get_holdings())
    
    def test_portfolio_value(self):
        """Test portfolio value calculation"""
        portfolio = Portfolio(initial_cash=100000.0)
        portfolio.buy("bitcoin", 0.5, 50000.0)
        portfolio.buy("ethereum", 10.0, 3000.0)
        
        prices = {"bitcoin": 55000.0, "ethereum": 3500.0}
        value = portfolio.get_portfolio_value(prices)
        
        expected = 100000.0 - 25000.0 - 30000.0 + (0.5 * 55000.0) + (10.0 * 3500.0)
        self.assertEqual(value, expected)


if __name__ == "__main__":
    unittest.main()
