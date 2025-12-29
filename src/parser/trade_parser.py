"""Trade data parsing and transformation logic."""

from __future__ import annotations

from typing import TYPE_CHECKING

from beartype import beartype

if TYPE_CHECKING:
    from collections.abc import Sequence

from src.parser.api_client import PolymarketAPIClient


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
