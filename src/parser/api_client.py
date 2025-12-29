"""Polymarket API client for HTTP requests."""

from __future__ import annotations

import time
from collections.abc import Mapping

from beartype import beartype
from httpx import Client, HTTPError, Response

from src.utils.config import API_RATE_LIMIT, POLYMARKET_API_V1_URL


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
    def get_trades(
        self,
        market_id: str,
        limit: int = 500,
        cursor: str | None = None,
    ) -> dict[str, object]:
        """
        Fetch trades for a specific market.

        Args:
            market_id: Polymarket market ID
            limit: Maximum number of trades to fetch
            cursor: Pagination cursor for fetching more results

        Returns:
            API response as a dictionary

        Raises:
            HTTPError: If the API request fails
        """
        params: dict[str, object] = {"market": market_id, "limit": limit}
        if cursor:
            params["cursor"] = cursor

        response = self._request("GET", "trades", params=params)
        return response.json()

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
    def get_event_markets(self, event_id: str) -> dict[str, object]:
        """
        Fetch all markets for a specific event.

        Args:
            event_id: Polymarket event ID (tid from URL query parameter)

        Returns:
            API response containing market data as a dictionary

        Raises:
            HTTPError: If the API request fails
        """
        params: dict[str, object] = {"event": event_id}
        response = self._request("GET", "markets", params=params)
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

