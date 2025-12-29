"""Script to fetch all trades from Polymarket API using async pagination."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from beartype import beartype

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import initialize_database
from src.database.repository import get_trade_count
from src.parser.api_client import AsyncPolymarketAPIClient
from src.parser.trade_parser import async_fetch_all_trades


def progress_callback(loaded: int, estimated_total: int) -> None:
    """Progress callback for async_fetch_all_trades."""
    print(f"Loaded {loaded} trades...", end="\r", flush=True)


@beartype
async def async_main(market_id: str, save_to_db: bool = True) -> None:
    """
    Async main function to fetch all trades for a market.

    Args:
        market_id: Polymarket market ID to fetch trades for
        save_to_db: Whether to save trades to database
    """
    print("=" * 60)
    print("Fetch ALL Trades from Polymarket API (Async Mode)")
    print("=" * 60)
    print(f"Market ID: {market_id}")
    print("-" * 60)

    # Initialize database if saving
    if save_to_db:
        print("Initializing database...")
        initialize_database()
        print("Database initialized.\n")

    # Get conditionId from numeric market_id if needed
    api_client = AsyncPolymarketAPIClient()
    try:
        print("Getting conditionId...")
        try:
            condition_id = await api_client.get_market_condition_id(market_id)
            print(f"Got conditionId: {condition_id}\n")
        except Exception as e:
            # If get_market_condition_id fails, assume market_id is already a conditionId
            print(f"Assuming market_id is conditionId (error: {e})\n")
            condition_id = market_id

        print("Starting to fetch all trades (this may take a while)...\n")

        # Fetch all trades using async function with real-time saving
        total_loaded = await async_fetch_all_trades(
            condition_id,
            api_client=api_client,
            limit_per_page=1000,
            save_to_db=save_to_db,
            progress_callback=progress_callback,
        )

        print("\n" + "=" * 60)
        print(f"✓ Successfully fetched {total_loaded} total trades")
        print("=" * 60)

        if total_loaded == 0:
            print("No trades found.")
            return

        # Verify (use condition_id for DB query since that's what we stored)
        if save_to_db:
            total_count = get_trade_count(condition_id)
            print(f"✓ Total trades in database for market {condition_id}: {total_count}")

        print("\n✓ Done!")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await api_client.close()


@beartype
def main(market_id: str, save_to_db: bool = True) -> None:
    """
    Main entry point (wrapper for async function).

    Args:
        market_id: Polymarket market ID to fetch trades for
        save_to_db: Whether to save trades to database
    """
    asyncio.run(async_main(market_id, save_to_db))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/fetch_all_trades.py <market_id> [--no-db]")
        print("\nArguments:")
        print("  market_id   Polymarket market ID")
        print("  --no-db     Don't save to database (only fetch and display)")
        print("\nExample:")
        print("  python scripts/fetch_all_trades.py 0x1234...")
        sys.exit(1)

    market_id = sys.argv[1]
    save_to_db = "--no-db" not in sys.argv

    main(market_id, save_to_db=save_to_db)

