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

