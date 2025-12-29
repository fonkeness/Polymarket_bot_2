"""Script to fetch all trades from Polymarket API using async pagination."""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

from beartype import beartype

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import initialize_database
from src.database.repository import get_trade_count
from src.parser.api_client import AsyncPolymarketAPIClient
from src.parser.trade_parser import async_fetch_all_trades

# Global variables for progress tracking
_start_time: float | None = None
_last_update_time: float | None = None
_last_loaded: int = 0
_speed_history: list[float] = []


def progress_callback(loaded: int, estimated_total: int) -> None:
    """Progress callback with speed calculation and ETA."""
    global _start_time, _last_update_time, _last_loaded, _speed_history

    current_time = time.time()

    if _start_time is None:
        _start_time = current_time
        _last_update_time = current_time
        _last_loaded = loaded
        print(f"Loaded {loaded:,} trades...", end="\r", flush=True)
        return

    # Calculate speed
    if _last_update_time is None:
        _last_update_time = current_time
        _last_loaded = loaded
        return
    
    time_delta = current_time - _last_update_time
    if time_delta > 0:
        instant_speed = (loaded - _last_loaded) / time_delta

        # Add to history (keep last 10 measurements)
        _speed_history.append(instant_speed)
        if len(_speed_history) > 10:
            _speed_history.pop(0)

        # Smoothed speed (average of last 10)
        avg_speed = sum(_speed_history) / len(_speed_history)

        # Overall average speed
        total_time = current_time - _start_time
        overall_speed = loaded / total_time if total_time > 0 else 0

        # ETA calculation
        if estimated_total > loaded and avg_speed > 0:
            remaining = estimated_total - loaded
            eta_seconds = remaining / avg_speed
            eta_min = int(eta_seconds // 60)
            eta_sec = int(eta_seconds % 60)
            eta_str = f" | ETA: {eta_min}m {eta_sec}s"
        else:
            eta_str = ""

        # Update every 0.3 seconds or every 200 trades
        if current_time - _last_update_time >= 0.3 or loaded - _last_loaded >= 200:
            print(
                f"Loaded {loaded:,} trades | "
                f"Speed: {avg_speed:.0f}/sec (avg: {overall_speed:.0f}/sec){eta_str}    ",
                end="\r",
                flush=True,
            )
            _last_update_time = current_time
            _last_loaded = loaded


@beartype
async def async_main(market_id: str, save_to_db: bool = True, max_trades: int = 1_000_000) -> None:
    """
    Async main function to fetch all trades for a market.

    Args:
        market_id: Polymarket market ID to fetch trades for
        save_to_db: Whether to save trades to database
        max_trades: Maximum number of trades to fetch (protection against infinite loops)
    """
    global _start_time, _last_update_time, _last_loaded, _speed_history

    # Reset progress tracking
    _start_time = None
    _last_update_time = None
    _last_loaded = 0
    _speed_history = []

    print("=" * 60)
    print("Fetch ALL Trades from Polymarket API (Optimized Mode)")
    print("=" * 60)
    print(f"Market ID: {market_id}")
    if max_trades < 1_000_000:
        print(f"Max trades limit: {max_trades:,}")
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

        # Fetch all trades using optimized async function
        # Use 500 as limit since Data API may have maximum limit of 500 per request
        total_loaded = await async_fetch_all_trades(
            condition_id,
            api_client=api_client,
            limit_per_page=500,
            save_to_db=save_to_db,
            progress_callback=progress_callback,
            max_trades=max_trades,
        )

        print("\n" + "=" * 60)
        print(f"✓ Successfully fetched {total_loaded:,} NEW trades")
        print("=" * 60)

        if total_loaded == 0:
            print("No new trades found (all were duplicates or already in database).")
            return

        # Verify (use condition_id for DB query since that's what we stored)
        if save_to_db:
            total_count = get_trade_count(condition_id)
            print(f"✓ Total unique trades in database for market {condition_id}: {total_count:,}")

        # Calculate final statistics
        if _start_time:
            total_time = time.time() - _start_time
            avg_speed = total_loaded / total_time if total_time > 0 else 0
            print(f"✓ Average speed: {avg_speed:.0f} trades/sec")
            print(f"✓ Total time: {int(total_time // 60)}m {int(total_time % 60)}s")

        print("\n✓ Done!")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await api_client.close()


@beartype
def main(market_id: str, save_to_db: bool = True, max_trades: int = 1_000_000) -> None:
    """
    Main entry point (wrapper for async function).

    Args:
        market_id: Polymarket market ID to fetch trades for
        save_to_db: Whether to save trades to database
        max_trades: Maximum number of trades to fetch
    """
    asyncio.run(async_main(market_id, save_to_db, max_trades))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/fetch_all_trades.py <market_id> [--no-db] [--max-trades N]")
        print("\nArguments:")
        print("  market_id      Polymarket market ID")
        print("  --no-db        Don't save to database (only fetch and display)")
        print("  --max-trades N Maximum number of trades to fetch (default: 1,000,000)")
        print("\nExample:")
        print("  python scripts/fetch_all_trades.py 0x1234...")
        print("  python scripts/fetch_all_trades.py 0x1234... --max-trades 500000")
        sys.exit(1)

    market_id = sys.argv[1]
    save_to_db = "--no-db" not in sys.argv

    # Parse --max-trades parameter
    max_trades = 1_000_000
    if "--max-trades" in sys.argv:
        idx = sys.argv.index("--max-trades")
        if idx + 1 < len(sys.argv):
            try:
                max_trades = int(sys.argv[idx + 1])
            except ValueError:
                print("Error: --max-trades must be a number")
                sys.exit(1)

    main(market_id, save_to_db=save_to_db, max_trades=max_trades)

