"""Script to fetch all trades from Polymarket API using pagination."""

from __future__ import annotations

import sys
from pathlib import Path

from beartype import beartype

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import initialize_database
from src.database.repository import get_trade_count, insert_trades_batch
from src.parser.api_client import PolymarketAPIClient
from src.parser.trade_parser import parse_trade_data


@beartype
def main(market_id: str, save_to_db: bool = True) -> None:
    """
    Main function to fetch all trades for a market.

    Args:
        market_id: Polymarket market ID to fetch trades for
        save_to_db: Whether to save trades to database
    """
    print("=" * 60)
    print("Fetch ALL Trades from Polymarket API")
    print("=" * 60)
    print(f"Market ID: {market_id}")
    print("-" * 60)

    # Initialize database if saving
    if save_to_db:
        print("Initializing database...")
        initialize_database()
        print("Database initialized.\n")

    # Fetch all trades with progress output
    api_client = PolymarketAPIClient()
    try:
        print("Starting to fetch all trades (this may take a while)...\n")
        
        # Manual pagination with progress output
        all_trades: list[tuple[int, float, float, str, str]] = []
        cursor: str | None = None
        page = 1
        
        while True:
            print(f"Fetching page {page}...", end=" ", flush=True)
            
            # Fetch trades with pagination
            response = api_client.get_trades(market_id, limit=500, cursor=cursor)
            
            # Extract trades from response
            trades_data = response.get("data", [])
            if not isinstance(trades_data, list):
                trades_data = []
            
            # Parse all trades from this page
            parsed_trades: list[tuple[int, float, float, str, str]] = []
            for trade in trades_data:
                parsed = parse_trade_data(trade)
                if parsed:
                    parsed_trades.append(parsed)
            
            all_trades.extend(parsed_trades)
            print(f"Got {len(parsed_trades)} trades (total: {len(all_trades)})")
            
            # Check for next page cursor
            next_cursor = response.get("cursor") or response.get("nextCursor")
            
            # If no more trades, we're done
            if not parsed_trades:
                break
            
            # If we got fewer trades than requested, this is the last page
            if len(parsed_trades) < 500:
                break
                
            # If no cursor available, we're done
            if not next_cursor:
                break
            
            cursor = str(next_cursor)
            page += 1

        print("\n" + "=" * 60)
        print(f"✓ Successfully fetched {len(all_trades)} total trades")
        print("=" * 60)

        if not all_trades:
            print("No trades found.")
            return

        # Save to database
        if save_to_db:
            print("\nSaving trades to database...")
            inserted_count = insert_trades_batch(all_trades)
            print(f"✓ Successfully inserted {inserted_count} trades into database.")

            # Verify
            total_count = get_trade_count(market_id)
            print(f"✓ Total trades in database for market {market_id}: {total_count}")

        print("\n✓ Done!")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        api_client.close()


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

