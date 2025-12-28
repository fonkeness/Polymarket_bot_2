"""Trade data parsing and transformation logic."""

from __future__ import annotations

from typing import TYPE_CHECKING

from beartype import beartype

if TYPE_CHECKING:
    from collections.abc import Sequence

from src.parser.api_client import PolymarketAPIClient


@beartype
def parse_trade_data(trade: dict[str, object]) -> tuple[int, float, float, str, str] | None:
    """
    Parse a single trade from API response to database format.

    Args:
        trade: Trade dictionary from API response

    Returns:
        Tuple of (timestamp, price, size, trader_address, market_id) or None if invalid
    """
    try:
        timestamp = int(trade.get("timestamp", 0))
        price = float(trade.get("price", 0.0))
        size = float(trade.get("size", 0.0))
        trader_address = str(trade.get("user", ""))
        market_id = str(trade.get("market", ""))

        # Validate required fields
        if not trader_address or not market_id or timestamp <= 0:
            return None

        return (timestamp, price, size, trader_address, market_id)
    except (ValueError, KeyError, TypeError):
        return None


@beartype
def fetch_trades(
    market_id: str,
    limit: int = 500,
    api_client: PolymarketAPIClient | None = None,
) -> list[tuple[int, float, float, str, str]]:
    """
    Fetch and parse trades from the Polymarket API.

    Args:
        market_id: Polymarket market ID
        limit: Maximum number of trades to fetch
        api_client: Optional API client (creates new if None)

    Returns:
        List of parsed trades as tuples (timestamp, price, size, trader_address, market_id)
    """
    should_close = api_client is None
    if api_client is None:
        api_client = PolymarketAPIClient()

    try:
        # Fetch trades from API
        response = api_client.get_trades(market_id, limit=limit)

        # Extract trades from response
        trades_data = response.get("data", [])
        if not isinstance(trades_data, list):
            trades_data = []

        # Parse all trades
        parsed_trades: list[tuple[int, float, float, str, str]] = []
        for trade in trades_data:
            parsed = parse_trade_data(trade)
            if parsed:
                parsed_trades.append(parsed)

        return parsed_trades
    finally:
        if should_close:
            api_client.close()


@beartype
def fetch_all_trades(
    market_id: str,
    api_client: PolymarketAPIClient | None = None,
    limit_per_page: int = 500,
) -> list[tuple[int, float, float, str, str]]:
    """
    Fetch ALL trades from Polymarket API using pagination.

    This function automatically handles pagination to fetch all available trades,
    not just the first page. Useful when you need to download all historical trades
    for a market (e.g., when hashdive.com shows 500+ pages of trades).

    Args:
        market_id: Polymarket market ID
        api_client: Optional API client (creates new if None)
        limit_per_page: Number of trades to fetch per page (max 500)

    Returns:
        List of all parsed trades as tuples (timestamp, price, size, trader_address, market_id)
    """
    should_close = api_client is None
    if api_client is None:
        api_client = PolymarketAPIClient()

    try:
        all_trades: list[tuple[int, float, float, str, str]] = []
        cursor: str | None = None
        page = 1

        while True:
            # Fetch trades with pagination
            response = api_client.get_trades(market_id, limit=limit_per_page, cursor=cursor)

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

            # Check for next page cursor
            next_cursor = response.get("cursor") or response.get("nextCursor")

            # If no more trades, we're done
            if not parsed_trades:
                break

            # If we got fewer trades than requested, this is the last page
            if len(parsed_trades) < limit_per_page:
                break

            # If no cursor available, we're done
            if not next_cursor:
                break

            cursor = str(next_cursor)
            page += 1

        return all_trades
    finally:
        if should_close:
            api_client.close()
