"""Data access layer for trade operations."""

from __future__ import annotations

from sqlite3 import Connection

from beartype import beartype

from src.database.connection import get_connection


@beartype
def insert_trade(
    timestamp: int,
    price: float,
    size: float,
    trader_address: str,
    market_id: str,
    conn: Connection | None = None,
) -> int:
    """
    Insert a single trade into the database.

    Args:
        timestamp: Unix timestamp of the trade
        price: Price of the trade
        size: Size/volume of the trade
        trader_address: Ethereum address of the trader
        market_id: Polymarket market ID
        conn: Optional database connection (creates new if None)

    Returns:
        The row ID of the inserted trade
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO trades (timestamp, price, size, trader_address, market_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (timestamp, price, size, trader_address, market_id),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        if should_close:
            conn.close()


@beartype
def insert_trades_batch(
    trades: list[tuple[int, float, float, str, str]],
    conn: Connection | None = None,
) -> int:
    """
    Insert multiple trades in a single transaction.

    Args:
        trades: List of tuples (timestamp, price, size, trader_address, market_id)
        conn: Optional database connection (creates new if None)

    Returns:
        Number of trades inserted
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    try:
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT OR IGNORE INTO trades (timestamp, price, size, trader_address, market_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            trades,
        )
        conn.commit()
        return cursor.rowcount
    finally:
        if should_close:
            conn.close()


@beartype
def get_trade_count(market_id: str | None = None) -> int:
    """
    Get the total number of trades in the database.

    Args:
        market_id: Optional market ID to filter by

    Returns:
        Total number of trades
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        if market_id:
            cursor.execute("SELECT COUNT(*) FROM trades WHERE market_id = ?", (market_id,))
        else:
            cursor.execute("SELECT COUNT(*) FROM trades")
        result = cursor.fetchone()
        return result[0] if result else 0
    finally:
        conn.close()


@beartype
def get_trades_by_market(market_id: str, limit: int | None = None) -> list[dict[str, object]]:
    """
    Retrieve trades for a specific market.

    Args:
        market_id: Market ID to filter by
        limit: Optional limit on number of trades to return

    Returns:
        List of trade dictionaries with keys: id, timestamp, price, size, trader_address, market_id, created_at
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        query = "SELECT * FROM trades WHERE market_id = ? ORDER BY timestamp DESC"
        if limit:
            query += f" LIMIT {limit}"
        cursor.execute(query, (market_id,))
        rows = cursor.fetchall()
        # Convert Row objects to dicts (row_factory is set to Row in connection.py)
        return [dict(row) for row in rows]
    finally:
        conn.close()

