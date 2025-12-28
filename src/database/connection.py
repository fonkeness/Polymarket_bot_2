"""Database connection management and initialization."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from sqlite3 import Connection

from beartype import beartype

from src.database.models import TRADES_INDEXES, TRADES_TABLE_SCHEMA
from src.utils.config import DB_PATH


@beartype
def get_connection() -> Connection:
    """Create and return a database connection."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


@beartype
def initialize_database() -> None:
    """Initialize the database with required tables and indexes."""
    # Ensure database directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    try:
        cursor = conn.cursor()

        # Create trades table
        cursor.execute(TRADES_TABLE_SCHEMA)

        # Create indexes
        for index_sql in TRADES_INDEXES:
            cursor.execute(index_sql)

        conn.commit()
    finally:
        conn.close()


@beartype
def database_exists() -> bool:
    """Check if the database file exists."""
    return DB_PATH.exists()

