"""Trade data parsing and transformation logic."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from beartype import beartype

if TYPE_CHECKING:
    from collections.abc import Sequence

from src.database.repository import insert_trades_batch
from src.parser.api_client import AsyncPolymarketAPIClient, PolymarketAPIClient


@beartype
def parse_trade_data(trade: dict[str, object], market_id: str) -> tuple[int, float, float, str, str] | None:
    """
    Parse a single trade from API response to database format.

    Args:
        trade: Trade dictionary from Data API response
        market_id: Market conditionId to associate with this trade

    Returns:
        Tuple of (timestamp, price, size, trader_address, market_id) or None if invalid
    """
    try:
        timestamp = int(trade.get("timestamp", 0))
        price = float(trade.get("price", 0.0))
        size = float(trade.get("size", 0.0))
        # Data API uses "proxyWallet" instead of "user"
        trader_address = str(trade.get("proxyWallet", ""))

        # Validate required fields
        if not trader_address or timestamp <= 0:
            return None

        return (timestamp, price, size, trader_address, market_id)
    except (ValueError, KeyError, TypeError):
        return None


@beartype
def fetch_trades(
    condition_id: str,
    limit: int = 500,
    api_client: PolymarketAPIClient | None = None,
) -> list[tuple[int, float, float, str, str]]:
    """
    Fetch and parse trades from the Polymarket Data API.

    Args:
        condition_id: Market conditionId (not numeric ID)
        limit: Maximum number of trades to fetch
        api_client: Optional API client (creates new if None)

    Returns:
        List of parsed trades as tuples (timestamp, price, size, trader_address, market_id)
    """
    should_close = api_client is None
    if api_client is None:
        api_client = PolymarketAPIClient()

    try:
        # Fetch trades from Data API (returns array directly)
        trades_data = api_client.get_trades(condition_id, limit=limit)

        # Parse all trades
        parsed_trades: list[tuple[int, float, float, str, str]] = []
        for trade in trades_data:
            parsed = parse_trade_data(trade, condition_id)
            if parsed:
                parsed_trades.append(parsed)

        return parsed_trades
    finally:
        if should_close:
            api_client.close()


@beartype
async def async_fetch_all_trades(
    condition_id: str,
    api_client: AsyncPolymarketAPIClient | None = None,
    limit_per_page: int = 1000,
    save_to_db: bool = True,
    progress_callback: Callable[[int, int], None] | None = None,
) -> int:
    """
    Fetch ALL trades from Polymarket Data API using async and offset pagination.

    This function uses offset-based pagination to fetch all historical trades.
    Results are sorted by timestamp descending (newest first).
    Trades are saved to database in real-time batches to avoid memory issues.

    Args:
        condition_id: Market conditionId (not numeric ID)
        api_client: Optional async API client (creates new if None)
        limit_per_page: Number of trades to fetch per page (max 1000 recommended)
        save_to_db: Whether to save trades to database in real-time
        progress_callback: Optional callback function(loaded_count, total_estimated) for progress

    Returns:
        Total number of trades fetched and saved
    """
    should_close = api_client is None
    if api_client is None:
        api_client = AsyncPolymarketAPIClient()

    try:
        total_loaded = 0
        offset = 0

        while True:
            # Fetch trades batch
            trades_data = await api_client.get_trades(
                condition_id, limit=limit_per_page, offset=offset
            )

            if not trades_data:
                # No more trades
                break

            # Parse all trades from this batch
            parsed_trades: list[tuple[int, float, float, str, str]] = []
            for trade in trades_data:
                parsed = parse_trade_data(trade, condition_id)
                if parsed:
                    parsed_trades.append(parsed)

            if not parsed_trades:
                # No valid trades in this batch
                break

            # Save to database in real-time
            if save_to_db:
                inserted_count = insert_trades_batch(parsed_trades)
                total_loaded += inserted_count
            else:
                total_loaded += len(parsed_trades)

            # Progress callback
            if progress_callback:
                # Estimate total (might be inaccurate, but gives user feedback)
                estimated_total = (
                    total_loaded + limit_per_page if len(trades_data) == limit_per_page else total_loaded
                )
                progress_callback(total_loaded, estimated_total)

            # Check if we got fewer trades than requested (last page)
            if len(trades_data) < limit_per_page:
                break

            # Move to next page
            offset += limit_per_page

        return total_loaded
    finally:
        if should_close:
            await api_client.close()


@beartype
def fetch_all_trades(
    condition_id: str,
    api_client: PolymarketAPIClient | None = None,
    limit_per_page: int = 500,
) -> list[tuple[int, float, float, str, str]]:
    """
    Fetch ALL trades from Polymarket Data API.

    Note: Data API doesn't support pagination with cursor, so this fetches all available
    trades up to the limit. For full historical data, you may need to call this multiple
    times or use a different approach.

    Args:
        condition_id: Market conditionId (not numeric ID)
        api_client: Optional API client (creates new if None)
        limit_per_page: Number of trades to fetch (Data API may have its own limits)

    Returns:
        List of all parsed trades as tuples (timestamp, price, size, trader_address, market_id)
    """
    should_close = api_client is None
    if api_client is None:
        api_client = PolymarketAPIClient()

    try:
        # Data API returns array directly, pagination may not be supported
        # Fetch with high limit to get as many trades as possible
        trades_data = api_client.get_trades(condition_id, limit=limit_per_page)

        # Parse all trades
        parsed_trades: list[tuple[int, float, float, str, str]] = []
        for trade in trades_data:
            parsed = parse_trade_data(trade, condition_id)
            if parsed:
                parsed_trades.append(parsed)

        return parsed_trades
    finally:
        if should_close:
            api_client.close()
