"""
SQLite Database Layer with ACID Compliance
Handles all data persistence with transaction safety
"""

import aiosqlite
import logging
from pathlib import Path
from typing import List, Optional, Any, Dict
from datetime import datetime
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class Database:
    """
    ACID-compliant SQLite database manager
    All writes are transactional
    """
    
    def __init__(self, db_path: str = "titan.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection: Optional[aiosqlite.Connection] = None
        
        logger.info(f"Database initialized: {db_path}")
    
    async def connect(self):
        """Establish database connection"""
        self._connection = await aiosqlite.connect(
            str(self.db_path),
            timeout=30.0
        )
        # Enable WAL mode for better concurrency
        await self._connection.execute("PRAGMA journal_mode=WAL")
        await self._connection.execute("PRAGMA foreign_keys=ON")
        
        logger.info("Database connected")
    
    async def close(self):
        """Close database connection"""
        if self._connection:
            await self._connection.close()
            logger.info("Database closed")
    
    @asynccontextmanager
    async def transaction(self):
        """
        Context manager for ACID transactions
        Automatically commits on success, rolls back on error
        """
        if not self._connection:
            raise RuntimeError("Database not connected")
        
        try:
            await self._connection.execute("BEGIN")
            yield self._connection
            await self._connection.commit()
        except Exception as e:
            await self._connection.rollback()
            logger.error(f"Transaction rolled back: {e}")
            raise
    
    async def initialize_schema(self):
        """Create database schema"""
        async with self.transaction() as conn:
            # Trades table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    price REAL NOT NULL CHECK(price > 0),
                    size REAL NOT NULL CHECK(size > 0),
                    side TEXT NOT NULL CHECK(side IN ('buy', 'sell')),
                    timestamp TEXT NOT NULL,
                    trade_id TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts 
                ON trades(symbol, timestamp)
            """)
            
            # Candles table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    open REAL NOT NULL CHECK(open > 0),
                    high REAL NOT NULL CHECK(high > 0),
                    low REAL NOT NULL CHECK(low > 0),
                    close REAL NOT NULL CHECK(close > 0),
                    volume REAL NOT NULL CHECK(volume >= 0),
                    start_ts TEXT NOT NULL,
                    end_ts TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, start_ts)
                )
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_candles_symbol_ts 
                ON candles(symbol, start_ts)
            """)
            
            # Wick events table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS wick_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    side TEXT NOT NULL CHECK(side IN ('upper', 'lower')),
                    wick_high REAL NOT NULL CHECK(wick_high > 0),
                    wick_low REAL NOT NULL CHECK(wick_low > 0),
                    wick_length REAL NOT NULL CHECK(wick_length > 0),
                    body_size REAL NOT NULL CHECK(body_size >= 0),
                    wick_to_body_ratio REAL NOT NULL CHECK(wick_to_body_ratio >= 0),
                    score REAL CHECK(score >= 0 AND score <= 100),
                    orderflow_delta REAL,
                    liquidity_imbalance REAL,
                    vwap_distance REAL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_wicks_symbol_ts 
                ON wick_events(symbol, timestamp)
            """)
            
            # Derivatives snapshots table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS derivatives_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    open_interest REAL NOT NULL CHECK(open_interest >= 0),
                    funding_rate REAL NOT NULL,
                    long_pct REAL NOT NULL CHECK(long_pct >= 0 AND long_pct <= 100),
                    short_pct REAL NOT NULL CHECK(short_pct >= 0 AND short_pct <= 100),
                    long_short_ratio REAL NOT NULL CHECK(long_short_ratio > 0),
                    long_liquidations_4h REAL NOT NULL CHECK(long_liquidations_4h >= 0),
                    short_liquidations_4h REAL NOT NULL CHECK(short_liquidations_4h >= 0),
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_derivs_symbol_ts 
                ON derivatives_snapshots(symbol, timestamp)
            """)
            
            # Strategy signals table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS strategy_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_name TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    price REAL NOT NULL CHECK(price > 0),
                    confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 100),
                    metadata TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_signals_strategy_ts 
                ON strategy_signals(strategy_name, timestamp)
            """)
        
        logger.info("Database schema initialized")
    
    async def insert_trade(self, trade_data: Dict[str, Any]) -> int:
        """Insert trade with ACID transaction"""
        async with self.transaction() as conn:
            cursor = await conn.execute("""
                INSERT INTO trades (symbol, price, size, side, timestamp, trade_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                trade_data['symbol'],
                trade_data['price'],
                trade_data['size'],
                trade_data['side'],
                trade_data['timestamp'].isoformat(),
                trade_data.get('trade_id')
            ))
            return cursor.lastrowid
    
    async def insert_wick_event(self, wick_data: Dict[str, Any]) -> int:
        """Insert wick event with ACID transaction"""
        async with self.transaction() as conn:
            cursor = await conn.execute("""
                INSERT INTO wick_events (
                    symbol, timestamp, side, wick_high, wick_low,
                    wick_length, body_size, wick_to_body_ratio, score,
                    orderflow_delta, liquidity_imbalance, vwap_distance
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                wick_data['symbol'],
                wick_data['timestamp'].isoformat(),
                wick_data['side'],
                wick_data['wick_high'],
                wick_data['wick_low'],
                wick_data['wick_length'],
                wick_data['body_size'],
                wick_data['wick_to_body_ratio'],
                wick_data.get('score'),
                wick_data.get('orderflow_delta'),
                wick_data.get('liquidity_imbalance'),
                wick_data.get('vwap_distance')
            ))
            return cursor.lastrowid
    
    async def query(self, sql: str, params: tuple = ()) -> List[Dict]:
        """Execute SELECT query and return results as dicts"""
        if not self._connection:
            raise RuntimeError("Database not connected")
        
        async with self._connection.execute(sql, params) as cursor:
            columns = [col[0] for col in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
