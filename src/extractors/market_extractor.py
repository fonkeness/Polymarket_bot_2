"""Market extraction logic for Polymarket events."""

from __future__ import annotations

from beartype import beartype

from src.extractors.models import Market
from src.extractors.url_parser import parse_event_url
from src.parser.api_client import PolymarketAPIClient


@beartype
def extract_markets(
    event_url: str,
    api_client: PolymarketAPIClient | None = None,
) -> list[Market]:
    """
    Extract all markets from a Polymarket event URL.

    Args:
        event_url: Polymarket event URL (e.g., https://polymarket.com/event/event-slug?tid=12345)
        api_client: Optional API client (creates new if None)

    Returns:
        List of Market objects containing market ID and name

    Raises:
        ValueError: If the URL format is invalid
        HTTPError: If the API request fails
    """
    should_close = api_client is None
    if api_client is None:
        api_client = PolymarketAPIClient()

    try:
        # Parse URL to get event slug
        event_slug = parse_event_url(event_url)

        # Query Gamma API for markets
        response = api_client.get_event_markets(event_slug)

        # Parse response - Gamma API returns markets in "markets" field
        markets_data = response.get("markets", [])
        if not isinstance(markets_data, list):
            markets_data = []

        # Extract market IDs and names
        markets: list[Market] = []
        for market_item in markets_data:
            if not isinstance(market_item, dict):
                continue

            # Gamma API uses "conditionId" for Data API market_id and "question" for name
            # If conditionId is present, use it directly. Otherwise use numeric "id" 
            # (which will need to be converted to conditionId later via get_market_condition_id)
            condition_id = market_item.get("conditionId")
            numeric_id = market_item.get("id")
            market_name = market_item.get("question")

            # Validate required fields
            if not market_name:
                continue

            # Prefer conditionId, but fallback to numeric id if needed
            # (numeric id will be converted to conditionId in stage2_main when fetching trades)
            market_id = condition_id if condition_id else numeric_id
            if not market_id:
                continue

            markets.append(Market(id=str(market_id), name=str(market_name)))

        return markets
    finally:
        if should_close:
            api_client.close()

