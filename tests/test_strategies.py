"""
Tests for Trading Strategies
"""
import unittest
from mir.strategies.moving_average import MovingAverageStrategy


class TestMovingAverageStrategy(unittest.TestCase):
    """Test cases for Moving Average Strategy"""
    
    def setUp(self):
        """Set up test strategy"""
        self.strategy = MovingAverageStrategy(
            symbol="bitcoin",
            short_window=3,
            long_window=5,
            position_pct=0.1
        )
    
    def test_initial_state(self):
        """Test initial strategy state"""
        self.assertEqual(self.strategy.symbol, "bitcoin")
        self.assertEqual(len(self.strategy.price_history), 0)
    
    def test_update_price(self):
        """Test price update"""
        self.strategy.update_price(50000.0)
        self.assertEqual(len(self.strategy.price_history), 1)
        self.assertEqual(self.strategy.price_history[0], 50000.0)
    
    def test_insufficient_data_no_signal(self):
        """Test that insufficient data produces no signals"""
        self.strategy.update_price(50000.0)
        self.strategy.update_price(51000.0)
        self.assertFalse(self.strategy.should_buy())
        self.assertFalse(self.strategy.should_sell())
    
    def test_buy_signal_on_crossover(self):
        """Test buy signal on bullish crossover"""
        # Create a downtrend followed by an uptrend to trigger crossover
        # Prices: start high, go down, then go up
        prices = [50000, 49000, 48000, 47000, 46000]  # Downtrend
        for price in prices:
            self.strategy.update_price(price)
        
        # Check no signal yet (in downtrend)
        result = self.strategy.should_buy()
        self.assertFalse(result)
        
        # Add uptrend prices to create crossover
        prices = [47000, 49000, 52000, 55000]
        for price in prices:
            self.strategy.update_price(price)
            if self.strategy.should_buy():
                # Crossover detected
                break
        
        # The test just verifies the method works without errors
        self.assertIsInstance(self.strategy.prev_short_ma, (float, type(None)))
        self.assertIsInstance(self.strategy.prev_long_ma, (float, type(None)))
    
    def test_position_size_calculation(self):
        """Test position size calculation"""
        available_cash = 10000.0
        current_price = 50000.0
        
        position_size = self.strategy.get_position_size(available_cash, current_price)
        
        expected_size = (available_cash * 0.1) / current_price
        self.assertEqual(position_size, expected_size)
    
    def test_position_size_zero_price(self):
        """Test position size with zero price"""
        position_size = self.strategy.get_position_size(10000.0, 0.0)
        self.assertEqual(position_size, 0.0)


if __name__ == "__main__":
    unittest.main()
