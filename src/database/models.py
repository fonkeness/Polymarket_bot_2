"""Database schema definitions for trade data."""

from __future__ import annotations

# SQL schema for trades table
# Note: We use INSERT OR IGNORE in repository.py to prevent duplicates
# Adding UNIQUE constraint would break existing databases, so we skip it
TRADES_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    price REAL NOT NULL,
    size REAL NOT NULL,
    trader_address TEXT NOT NULL,
    market_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# Index for faster queries
TRADES_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_trader_address ON trades(trader_address)",
    "CREATE INDEX IF NOT EXISTS idx_market_id ON trades(market_id)",
    "CREATE INDEX IF NOT EXISTS idx_timestamp ON trades(timestamp)",
]

