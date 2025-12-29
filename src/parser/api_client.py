"""Polymarket API client for HTTP requests."""

from __future__ import annotations

import time
from collections.abc import Mapping

from beartype import beartype
from httpx import Client, HTTPError, Response

from src.utils.config import API_RATE_LIMIT, POLYMARKET_API_V1_URL, POLYMARKET_GAMMA_API_URL, POLYMARKET_DATA_API_URL


class PolymarketAPIClient:
    """Client for interacting with the Polymarket API."""

    def __init__(self, base_url: str = POLYMARKET_API_V1_URL, rate_limit: float = API_RATE_LIMIT) -> None:
        """
        Initialize the API client.

        Args:
            base_url: Base URL for the Polymarket API
            rate_limit: Maximum requests per second
        """
        self.base_url = base_url
        self.rate_limit = rate_limit
        self.last_request_time = 0.0
        self.client = Client(timeout=30.0)

    def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to respect rate limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 1.0 / self.rate_limit

        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    @beartype
    def _request(
        self,
        method: str,
        endpoint: str,
        params: Mapping[str, object] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Response:
        """
        Make an HTTP request to the API.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (relative to base_url)
            params: Query parameters
            headers: Request headers

        Returns:
            HTTP response object

        Raises:
            HTTPError: If the request fails
        """
        self._wait_for_rate_limit()

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self.client.request(method, url, params=params, headers=headers)
        response.raise_for_status()
        return response

    @beartype
    def get_market_condition_id(self, market_id: str) -> str:
        """
        Get conditionId for a market using its numeric ID from Gamma API.

        Args:
            market_id: Numeric market ID from Gamma API

        Returns:
            conditionId string

        Raises:
            HTTPError: If the API request fails
        """
        url = f"{POLYMARKET_GAMMA_API_URL}/markets/{market_id}"
        self._wait_for_rate_limit()
        response = self.client.get(url)
        response.raise_for_status()
        data = response.json()
        condition_id = data.get("conditionId")
        if not condition_id:
            raise ValueError(f"conditionId not found for market {market_id}")
        return str(condition_id)

    @beartype
    def get_trades(
        self,
        condition_id: str,
        limit: int = 500,
        cursor: str | None = None,
    ) -> list[dict[str, object]]:
        """
        Fetch trades for a specific market using conditionId via Data API.

        Args:
            condition_id: Market conditionId (not numeric ID)
            limit: Maximum number of trades to fetch
            cursor: Pagination cursor (not used by data-api, kept for compatibility)

        Returns:
            List of trade dictionaries (data-api returns array directly)

        Raises:
            HTTPError: If the API request fails
        """
        url = f"{POLYMARKET_DATA_API_URL}/trades"
        params: dict[str, object] = {"market": condition_id, "limit": limit}
        self._wait_for_rate_limit()
        response = self.client.get(url, params=params)
        response.raise_for_status()
        trades = response.json()
        # Data API returns array directly, not wrapped in object
        if not isinstance(trades, list):
            return []
        return trades

    @beartype
    def get_market_info(self, market_id: str) -> dict[str, object]:
        """
        Fetch information about a specific market.

        Args:
            market_id: Polymarket market ID

        Returns:
            Market information as a dictionary

        Raises:
            HTTPError: If the API request fails
        """
        response = self._request("GET", f"markets/{market_id}")
        return response.json()

    @beartype
    def get_event_markets(self, event_slug: str) -> dict[str, object]:
        """
        Fetch all markets for a specific event using Gamma API.

        Args:
            event_slug: Polymarket event slug (e.g., "fed-decision-in-january")

        Returns:
            API response containing market data as a dictionary

        Raises:
            HTTPError: If the API request fails
        """
        # Use Gamma API endpoint: /events/slug/{slug}
        url = f"{POLYMARKET_GAMMA_API_URL}/events/slug/{event_slug}"
        self._wait_for_rate_limit()
        response = self.client.get(url)
        response.raise_for_status()
        return response.json()

    def close(self) -> None:
        """Close the HTTP client."""
        self.client.close()

    def __enter__(self) -> PolymarketAPIClient:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Context manager exit."""
        self.close()

