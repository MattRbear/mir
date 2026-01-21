"""
Pydantic Data Models for Titan
All data ingress/egress must use these validated models
"""

from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum


# ============================================================================
# Enums
# ============================================================================

class WickSide(str, Enum):
    """Wick direction"""
    UPPER = "upper"
    LOWER = "lower"


class ObjectType(str, Enum):
    """Tradeable object types"""
    WICK = "wick"
    POOR_HIGH = "poor_high"
    POOR_LOW = "poor_low"
    BOX = "box"


class BoxState(str, Enum):
    """Box state"""
    ACTIVE = "active"
    BROKEN_UP = "broken_up"
    BROKEN_DOWN = "broken_down"


class StrategyStatus(str, Enum):
    """Strategy health status"""
    INITIALIZING = "initializing"
    RUNNING = "running"
    ERROR = "error"
    STOPPED = "stopped"


# ============================================================================
# Market Data Models
# ============================================================================

class Trade(BaseModel):
    """Individual trade"""
    symbol: str
    price: float = Field(gt=0)
    size: float = Field(gt=0)
    side: str  # "buy" or "sell"
    timestamp: datetime
    trade_id: Optional[str] = None


class Candle(BaseModel):
    """OHLCV Candle"""
    symbol: str
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)
    start_ts: datetime
    end_ts: datetime
    trades: List[Trade] = Field(default_factory=list)
    
    @field_validator('high')
    @classmethod
    def validate_high(cls, v, info):
        """Ensure high >= open, close, low"""
        if 'open' in info.data and v < info.data['open']:
            raise ValueError("high must be >= open")
        if 'close' in info.data and v < info.data['close']:
            raise ValueError("high must be >= close")
        if 'low' in info.data and v < info.data['low']:
            raise ValueError("high must be >= low")
        return v


class OrderBook(BaseModel):
    """Order book snapshot"""
    symbol: str
    timestamp: datetime
    bids: List[tuple[float, float]]  # [(price, size), ...]
    asks: List[tuple[float, float]]
    best_bid: float = Field(gt=0)
    best_ask: float = Field(gt=0)
    
    @field_validator('best_ask')
    @classmethod
    def validate_spread(cls, v, info):
        """Ensure ask > bid"""
        if 'best_bid' in info.data and v <= info.data['best_bid']:
            raise ValueError("best_ask must be > best_bid")
        return v


# ============================================================================
# Event Models
# ============================================================================

class WickEvent(BaseModel):
    """Detected wick event"""
    symbol: str
    timestamp: datetime
    side: WickSide
    wick_high: float = Field(gt=0)
    wick_low: float = Field(gt=0)
    wick_length: float = Field(gt=0)
    body_size: float = Field(ge=0)
    wick_to_body_ratio: float = Field(ge=0)
    score: Optional[float] = Field(default=None, ge=0, le=100)
    
    # Feature data
    orderflow_delta: Optional[float] = None
    liquidity_imbalance: Optional[float] = None
    vwap_distance: Optional[float] = None


class ObjectEvent(BaseModel):
    """Tradeable object event"""
    symbol: str
    timestamp: datetime
    object_type: ObjectType
    price: float = Field(gt=0)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BoxEvent(BaseModel):
    """Consolidation box"""
    symbol: str
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    mid: float = Field(gt=0)
    range: float = Field(gt=0)
    duration_candles: int = Field(gt=0)
    state: BoxState
    start_ts: datetime
    end_ts: datetime


# ============================================================================
# Derivatives Models
# ============================================================================

class DerivativesSnapshot(BaseModel):
    """Derivatives market snapshot"""
    symbol: str
    timestamp: datetime
    open_interest: float = Field(ge=0)
    funding_rate: float
    long_pct: float = Field(ge=0, le=100)
    short_pct: float = Field(ge=0, le=100)
    long_short_ratio: float = Field(gt=0)
    long_liquidations_4h: float = Field(ge=0)
    short_liquidations_4h: float = Field(ge=0)


class WhaleTransaction(BaseModel):
    """Large whale transaction"""
    symbol: str
    timestamp: datetime
    amount_usd: float = Field(gt=0)
    from_address: str
    to_address: str
    transaction_type: str  # "exchange_inflow", "exchange_outflow", "transfer"
    exchange: Optional[str] = None


# ============================================================================
# Strategy Models
# ============================================================================

class StrategySignal(BaseModel):
    """Trading signal from strategy"""
    strategy_name: str
    symbol: str
    timestamp: datetime
    signal_type: str  # "wick_magnet", "poor_level", "box_breakout", etc.
    price: float = Field(gt=0)
    confidence: float = Field(ge=0, le=100)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class HealthStatus(BaseModel):
    """Strategy health status"""
    strategy_name: str
    status: StrategyStatus
    last_heartbeat: datetime
    uptime_seconds: int = Field(ge=0)
    events_processed: int = Field(ge=0)
    errors_count: int = Field(ge=0)
    last_error: Optional[str] = None
    pnl: Optional[float] = None


# ============================================================================
# System Models
# ============================================================================

class SystemMetrics(BaseModel):
    """System-wide metrics"""
    timestamp: datetime
    cpu_percent: float = Field(ge=0, le=100)
    ram_percent: float = Field(ge=0, le=100)
    uptime_seconds: int = Field(ge=0)
    active_strategies: int = Field(ge=0)
    total_events: int = Field(ge=0)
