"""Main entry point for Stage 2: Database Setup & Initial Parsing."""

from __future__ import annotations

import sys
from pathlib import Path

from beartype import beartype

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import initialize_database
from src.database.repository import get_trade_count, insert_trades_batch
from src.parser.api_client import PolymarketAPIClient
from src.parser.trade_parser import fetch_trades
from src.utils.config import INITIAL_TRADE_LIMIT


@beartype
def main(market_id: str) -> None:
    """
    Main function to test Stage 2 functionality.

    Args:
        market_id: Polymarket market ID to fetch trades for
    """
    print("Stage 2: Database Setup & Initial Parsing")
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
        # Get conditionId from numeric market_id if needed
        api_client = PolymarketAPIClient()
        try:
            condition_id = api_client.get_market_condition_id(market_id)
            print(f"Got conditionId: {condition_id}")
        except Exception as e:
            # If get_market_condition_id fails, assume market_id is already a conditionId
            print(f"Assuming market_id is conditionId (error getting conditionId: {e})")
            condition_id = market_id
        finally:
            api_client.close()

        trades = fetch_trades(condition_id, limit=INITIAL_TRADE_LIMIT)
        print(f"Fetched {len(trades)} trades from API.")

        if not trades:
            print("No trades found. Exiting.")
            return

        # Save to database
        print("\nSaving trades to database...")
        inserted_count = insert_trades_batch(trades)
        print(f"Successfully inserted {inserted_count} trades into database.")

        # Verify (use condition_id for DB query since that's what we stored)
        total_count = get_trade_count(condition_id)
        print(f"\nTotal trades in database for market {condition_id}: {total_count}")

        print("\nStage 2 completed successfully!")
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/stage2_main.py <market_id>")
        print("Example: python scripts/stage2_main.py 0x1234...")
        sys.exit(1)

    market_id = sys.argv[1]
    main(market_id)


