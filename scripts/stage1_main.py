"""Main entry point for Stage 1 testing."""

from __future__ import annotations

import sys
from pathlib import Path

from beartype import beartype

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import initialize_database
from src.database.repository import get_trade_count, insert_trades_batch
from src.parser.trade_parser import fetch_trades
from src.utils.config import INITIAL_TRADE_LIMIT


@beartype
def main(market_id: str) -> None:
    """
    Main function to test Stage 1 functionality.

    Args:
        market_id: Polymarket market ID to fetch trades for
    """
    print(f"Stage 1: Database Setup & Initial Parsing")
    print(f"Market ID: {market_id}")
    print(f"Trade Limit: {INITIAL_TRADE_LIMIT}")
    print("-" * 50)

    # Initialize database
    print("Initializing database...")
    initialize_database()
    print("Database initialized successfully.")

    # Fetch trades
    print(f"\nFetching up to {INITIAL_TRADE_LIMIT} trades from Polymarket API...")
    try:
        trades = fetch_trades(market_id, limit=INITIAL_TRADE_LIMIT)
        print(f"Fetched {len(trades)} trades from API.")

        if not trades:
            print("No trades found. Exiting.")
            return

        # Save to database
        print("\nSaving trades to database...")
        inserted_count = insert_trades_batch(trades)
        print(f"Successfully inserted {inserted_count} trades into database.")

        # Verify
        total_count = get_trade_count(market_id)
        print(f"\nTotal trades in database for market {market_id}: {total_count}")

        print("\nStage 1 completed successfully!")
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/stage1_main.py <market_id>")
        print("Example: python scripts/stage1_main.py 0x1234...")
        sys.exit(1)

    market_id = sys.argv[1]
    main(market_id)

