"""
Tests for Configuration module
"""
import unittest
import tempfile
import os
from mir.config import Config


class TestConfig(unittest.TestCase):
    """Test cases for Config class"""
    
    def test_default_config(self):
        """Test default configuration"""
        config = Config()
        self.assertEqual(config.get("portfolio.initial_cash"), 10000.0)
        self.assertIsNotNone(config.get("data_fetcher.base_url"))
    
    def test_get_nested_key(self):
        """Test getting nested configuration values"""
        config = Config()
        initial_cash = config.get("portfolio.initial_cash")
        self.assertIsInstance(initial_cash, float)
    
    def test_get_with_default(self):
        """Test getting non-existent key with default"""
        config = Config()
        value = config.get("nonexistent.key", "default_value")
        self.assertEqual(value, "default_value")
    
    def test_load_from_yaml(self):
        """Test loading configuration from YAML file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
portfolio:
  initial_cash: 5000.0
engine:
  iterations: 5
""")
            temp_file = f.name
        
        try:
            config = Config.from_file(temp_file)
            self.assertEqual(config.get("portfolio.initial_cash"), 5000.0)
            self.assertEqual(config.get("engine.iterations"), 5)
        finally:
            os.unlink(temp_file)
    
    def test_load_nonexistent_file(self):
        """Test loading from non-existent file uses defaults"""
        config = Config.from_file("nonexistent_file.yaml")
        self.assertEqual(config.get("portfolio.initial_cash"), 10000.0)


if __name__ == "__main__":
    unittest.main()
