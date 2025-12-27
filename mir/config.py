"""
Configuration management for the trading system
"""
import yaml
from typing import Dict, Any, Optional


class Config:
    """Configuration manager"""
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        """
        Initialize configuration
        
        Args:
            config_dict: Configuration dictionary
        """
        self.config = config_dict or self._default_config()
    
    @staticmethod
    def _default_config() -> Dict[str, Any]:
        """Return default configuration"""
        return {
            "portfolio": {
                "initial_cash": 10000.0
            },
            "data_fetcher": {
                "base_url": "https://api.coingecko.com/api/v3"
            },
            "strategies": [
                {
                    "type": "moving_average",
                    "symbol": "bitcoin",
                    "params": {
                        "short_window": 5,
                        "long_window": 20,
                        "position_pct": 0.1
                    }
                }
            ],
            "engine": {
                "iterations": 10,
                "interval": 60
            }
        }
    
    @classmethod
    def from_file(cls, filepath: str) -> "Config":
        """
        Load configuration from YAML file
        
        Args:
            filepath: Path to configuration file
            
        Returns:
            Config instance
        """
        try:
            with open(filepath, 'r') as f:
                config_dict = yaml.safe_load(f)
            return cls(config_dict)
        except FileNotFoundError:
            print(f"Config file {filepath} not found, using defaults")
            return cls()
        except Exception as e:
            print(f"Error loading config: {e}, using defaults")
            return cls()
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value
    
    def save(self, filepath: str):
        """
        Save configuration to YAML file
        
        Args:
            filepath: Path to save configuration
        """
        with open(filepath, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False)
