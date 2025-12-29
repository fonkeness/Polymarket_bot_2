"""Market extraction logic for Polymarket events."""

from __future__ import annotations

import re

from beartype import beartype
from httpx import HTTPStatusError

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
        event_url: Polymarket event URL (e.g., https://polymarket.com/event/event-slug)
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

        # Query API for markets - try with original slug first
        try:
            response = api_client.get_event_markets(event_slug)
        except HTTPStatusError as e:
            # If 404, try without numeric suffix (e.g., "slug-123" -> "slug")
            # Many Polymarket URLs have numeric suffixes that may not work with API
            if e.response.status_code == 404:
                # Try removing trailing numeric suffix
                slug_without_suffix = re.sub(r'-\d+$', '', event_slug)
                if slug_without_suffix != event_slug and slug_without_suffix:
                    try:
                        response = api_client.get_event_markets(slug_without_suffix)
                    except Exception:
                        # Re-raise original error if fallback also fails
                        raise e
                else:
                    raise e
            else:
                raise e

        # Parse response to extract market data
        markets_data = response.get("data", [])
        if not isinstance(markets_data, list):
            markets_data = []

        # Extract market IDs and names
        markets: list[Market] = []
        for market_item in markets_data:
            if not isinstance(market_item, dict):
                continue

            market_id = market_item.get("id") or market_item.get("market_id")
            market_name = market_item.get("name") or market_item.get("title") or market_item.get("question")

            # Validate required fields
            if not market_id or not market_name:
                continue

            # Ensure both are strings
            try:
                market_id_str = str(market_id)
                market_name_str = str(market_name)
            except (TypeError, ValueError):
                continue

            if market_id_str and market_name_str:
                markets.append(Market(id=market_id_str, name=market_name_str))

        return markets
    finally:
        if should_close:
            api_client.close()

