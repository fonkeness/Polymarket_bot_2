"""Polymarket API client for HTTP requests."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping

from beartype import beartype
from httpx import AsyncClient, Client, HTTPError, Response

from src.utils.config import (
    API_RATE_LIMIT,
    POLYMARKET_API_V1_URL,
    POLYMARKET_DATA_API_URL,
    POLYMARKET_GAMMA_API_URL,
    THE_GRAPH_API_URL,
)


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
        skip: int = 0,
        max_retries: int = 3,
    ) -> list[dict[str, object]]:
        """
        Fetch trades for a specific market using conditionId via The Graph API.
        Includes retry logic for indexer errors.

        Args:
            condition_id: Market conditionId (not numeric ID)
            limit: Maximum number of trades to fetch
            cursor: Pagination cursor (not used, kept for compatibility)
            skip: Number of trades to skip (for pagination)
            max_retries: Maximum number of retry attempts for indexer errors

        Returns:
            List of trade dictionaries from The Graph API

        Raises:
            HTTPError: If the API request fails after all retries
        """
        # GraphQL query to fetch trades for a specific market
        graphql_query = """
        query GetTrades($marketId: String!, $first: Int!, $skip: Int!) {
            trades(
                first: $first
                skip: $skip
                where: { market: $marketId }
                orderBy: timestamp
                orderDirection: desc
            ) {
                id
                market {
                    id
                }
                outcomeIndex
                price
                amount
                timestamp
                user {
                    id
                }
                side
            }
        }
        """
        
        variables = {
            "marketId": condition_id,
            "first": limit,
            "skip": skip,
        }
        
        payload = {
            "query": graphql_query,
            "variables": variables,
        }
        
        headers = {"Content-Type": "application/json"}
        
        # Retry logic for indexer errors
        last_error = None
        for attempt in range(max_retries):
            try:
                self._wait_for_rate_limit()
                response = self.client.post(THE_GRAPH_API_URL, json=payload, headers=headers)
                response.raise_for_status()
                
                result = response.json()
                
                # Check for errors in response
                if "errors" in result:
                    error_msg = "; ".join(str(err) for err in result["errors"])
                    error_str = str(error_msg).lower()
                    
                    # Check if it's an indexer error (retryable)
                    if "bad indexers" in error_str or "unavailable" in error_str or "too far behind" in error_str:
                        if attempt < max_retries - 1:
                            # Exponential backoff: 2^attempt seconds
                            wait_time = 2 ** attempt
                            print(f"Warning: The Graph indexer error (attempt {attempt + 1}/{max_retries}). Retrying in {wait_time}s...")
                            time.sleep(wait_time)
                            last_error = error_msg
                            continue
                        else:
                            print(f"Error: The Graph indexer unavailable after {max_retries} attempts. Returning empty list.")
                            print(f"DEBUG: Full API response: {result}")
                            return []  # Return empty list instead of raising error
                    # Check if it's a schema error (may be temporary - retryable)
                    elif "no field" in error_str or "unknown field" in error_str:
                        if attempt < max_retries - 1:
                            # Exponential backoff: 2^attempt seconds
                            wait_time = 2 ** attempt
                            print(f"Warning: The Graph schema error (attempt {attempt + 1}/{max_retries}). Retrying in {wait_time}s...")
                            print(f"DEBUG: Full API response: {result}")
                            time.sleep(wait_time)
                            last_error = error_msg
                            continue
                        else:
                            # After all retries, return empty list instead of crashing
                            print(f"Error: The Graph schema error after {max_retries} attempts. Field 'trades' not found.")
                            print(f"DEBUG: Full API response: {result}")
                            print(f"Warning: This may indicate the subgraph schema has changed or the subgraph ID is incorrect.")
                            return []  # Return empty list instead of raising error
                    else:
                        # Non-retryable error - but still return empty list instead of crashing
                        print(f"Warning: The Graph API error: {error_msg}")
                        print(f"DEBUG: Full API response: {result}")
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            print(f"Retrying in {wait_time}s...")
                            time.sleep(wait_time)
                            last_error = error_msg
                            continue
                        else:
                            print(f"Error: Returning empty list after {max_retries} attempts.")
                            return []  # Return empty list instead of raising error
                
                # The Graph returns data in {"data": {"trades": [...]}} format
                # Check if data exists and is not None before checking for trades
                if "data" in result and result["data"] is not None and "trades" in result["data"]:
                    return result["data"]["trades"]
                
                # Log unexpected response format for debugging
                print(f"DEBUG: Unexpected response format. Full response: {result}")
                if "data" in result and result["data"] is None:
                    print(f"Warning: The Graph API returned data=null for market {condition_id}.")
                
                return []
                
            except HTTPError as e:
                # If it's the last attempt, raise the error
                if attempt == max_retries - 1:
                    raise
                # Otherwise, retry with exponential backoff
                wait_time = 2 ** attempt
                print(f"Warning: HTTP error (attempt {attempt + 1}/{max_retries}). Retrying in {wait_time}s...")
                time.sleep(wait_time)
                last_error = str(e)
        
        # If we get here, all retries failed
        if last_error:
            raise HTTPError(f"The Graph API error after {max_retries} attempts: {last_error}")
        return []

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


class AsyncPolymarketAPIClient:
    """Async client for interacting with the Polymarket API."""

    def __init__(self, rate_limit: float = API_RATE_LIMIT) -> None:
        """
        Initialize the async API client.

        Args:
            rate_limit: Maximum requests per second
        """
        self.rate_limit = rate_limit
        self.last_request_time = 0.0
        self.client = AsyncClient(timeout=30.0)
        self._rate_limit_lock = asyncio.Lock()

    async def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to respect rate limits."""
        async with self._rate_limit_lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            min_interval = 1.0 / self.rate_limit

            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                await asyncio.sleep(sleep_time)

            self.last_request_time = time.time()

    @beartype
    async def get_market_condition_id(self, market_id: str) -> str:
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
        await self._wait_for_rate_limit()
        response = await self.client.get(url)
        response.raise_for_status()
        data = response.json()
        condition_id = data.get("conditionId")
        if not condition_id:
            raise ValueError(f"conditionId not found for market {market_id}")
        return str(condition_id)

    @beartype
    async def get_market_info(self, market_id: str) -> dict[str, object]:
        """
        Fetch market information from Gamma API.

        Args:
            market_id: Numeric market ID from Gamma API

        Returns:
            Market information dictionary (may contain createdAt, created_at, startDate, etc.)

        Raises:
            HTTPError: If the API request fails
        """
        url = f"{POLYMARKET_GAMMA_API_URL}/markets/{market_id}"
        await self._wait_for_rate_limit()
        response = await self.client.get(url)
        response.raise_for_status()
        return response.json()

    @beartype
    async def get_trades(
        self,
        condition_id: str,
        limit: int = 500,
        offset: int = 0,
        max_retries: int = 3,
    ) -> list[dict[str, object]]:
        """
        Fetch trades for a specific market using conditionId via The Graph API with skip pagination.
        Includes retry logic for indexer errors.

        Args:
            condition_id: Market conditionId (not numeric ID)
            limit: Maximum number of trades to fetch per page
            offset: Number of trades to skip (for pagination)
            max_retries: Maximum number of retry attempts for indexer errors

        Returns:
            List of trade dictionaries from The Graph API

        Raises:
            HTTPError: If the API request fails after all retries
        """
        # GraphQL query to fetch trades for a specific market
        graphql_query = """
        query GetTrades($marketId: String!, $first: Int!, $skip: Int!) {
            trades(
                first: $first
                skip: $skip
                where: { market: $marketId }
                orderBy: timestamp
                orderDirection: desc
            ) {
                id
                market {
                    id
                }
                outcomeIndex
                price
                amount
                timestamp
                user {
                    id
                }
                side
            }
        }
        """
        
        variables = {
            "marketId": condition_id,
            "first": limit,
            "skip": offset,
        }
        
        payload = {
            "query": graphql_query,
            "variables": variables,
        }
        
        headers = {"Content-Type": "application/json"}
        
        # Retry logic for indexer errors
        last_error = None
        for attempt in range(max_retries):
            try:
                await self._wait_for_rate_limit()
                response = await self.client.post(THE_GRAPH_API_URL, json=payload, headers=headers)
                response.raise_for_status()
                
                result = response.json()
                
                # Check for errors in response
                if "errors" in result:
                    error_msg = "; ".join(str(err) for err in result["errors"])
                    error_str = str(error_msg).lower()
                    
                    # Check if it's an indexer error (retryable)
                    if "bad indexers" in error_str or "unavailable" in error_str or "too far behind" in error_str:
                        if attempt < max_retries - 1:
                            # Exponential backoff: 2^attempt seconds
                            wait_time = 2 ** attempt
                            print(f"Warning: The Graph indexer error (attempt {attempt + 1}/{max_retries}). Retrying in {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            last_error = error_msg
                            continue
                        else:
                            print(f"Error: The Graph indexer unavailable after {max_retries} attempts. Returning empty list.")
                            print(f"DEBUG: Full API response: {result}")
                            return []  # Return empty list instead of raising error
                    # Check if it's a schema error (may be temporary - retryable)
                    elif "no field" in error_str or "unknown field" in error_str:
                        if attempt < max_retries - 1:
                            # Exponential backoff: 2^attempt seconds
                            wait_time = 2 ** attempt
                            print(f"Warning: The Graph schema error (attempt {attempt + 1}/{max_retries}). Retrying in {wait_time}s...")
                            print(f"DEBUG: Full API response: {result}")
                            await asyncio.sleep(wait_time)
                            last_error = error_msg
                            continue
                        else:
                            # After all retries, return empty list instead of crashing
                            print(f"Error: The Graph schema error after {max_retries} attempts. Field 'trades' not found.")
                            print(f"DEBUG: Full API response: {result}")
                            print(f"Warning: This may indicate the subgraph schema has changed or the subgraph ID is incorrect.")
                            return []  # Return empty list instead of raising error
                    else:
                        # Non-retryable error - but still return empty list instead of crashing
                        print(f"Warning: The Graph API error: {error_msg}")
                        print(f"DEBUG: Full API response: {result}")
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            print(f"Retrying in {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            last_error = error_msg
                            continue
                        else:
                            print(f"Error: Returning empty list after {max_retries} attempts.")
                            return []  # Return empty list instead of raising error
                
                # The Graph returns data in {"data": {"trades": [...]}} format
                # Check if data exists and is not None before checking for trades
                if "data" in result and result["data"] is not None and "trades" in result["data"]:
                    return result["data"]["trades"]
                
                # Log unexpected response format for debugging
                print(f"DEBUG: Unexpected response format. Full response: {result}")
                if "data" in result and result["data"] is None:
                    print(f"Warning: The Graph API returned data=null for market {condition_id}.")
                
                return []
                
            except HTTPError as e:
                # If it's the last attempt, raise the error
                if attempt == max_retries - 1:
                    raise
                # Otherwise, retry with exponential backoff
                wait_time = 2 ** attempt
                print(f"Warning: HTTP error (attempt {attempt + 1}/{max_retries}). Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                last_error = str(e)
        
        # If we get here, all retries failed - try REST API fallback
        print(f"Warning: The Graph API failed after {max_retries} attempts.")
        print(f"Attempting fallback to Polymarket REST API...")
        return await self._get_trades_via_rest_api(condition_id, limit, offset)

    async def _get_trades_via_rest_api(
        self,
        condition_id: str,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        """
        Fallback method to fetch trades via Polymarket REST API.
        
        Args:
            condition_id: Market conditionId
            limit: Maximum number of trades to fetch
            offset: Number of trades to skip
            
        Returns:
            List of trade dictionaries in The Graph format
        """
        try:
            url = f"{POLYMARKET_DATA_API_URL}/trades"
            params = {
                "conditionId": condition_id,
                "limit": limit,
                "offset": offset,
            }
            
            await self._wait_for_rate_limit()
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            
            trades_data = response.json()
            
            # REST API returns list directly or wrapped in data field
            if isinstance(trades_data, dict) and "data" in trades_data:
                trades_data = trades_data["data"]
            
            if not isinstance(trades_data, list):
                print(f"Warning: REST API returned unexpected format: {type(trades_data)}")
                return []
            
            # Convert REST API format to The Graph format
            converted_trades = []
            for trade in trades_data:
                # REST API format: {proxyWallet, side, asset, conditionId, size, price, timestamp, ...}
                # The Graph format: {id, market: {id}, outcomeIndex, price, amount, timestamp, user: {id}, side}
                converted_trade = {
                    "id": trade.get("id") or f"{trade.get('timestamp', 0)}_{trade.get('proxyWallet', '')}",
                    "market": {
                        "id": trade.get("conditionId") or condition_id,
                    },
                    "outcomeIndex": trade.get("outcomeIndex") or 0,
                    "price": float(trade.get("price", 0.0)),
                    "amount": float(trade.get("size", trade.get("amount", 0.0))),
                    "timestamp": int(trade.get("timestamp", 0)),
                    "user": {
                        "id": trade.get("proxyWallet") or trade.get("user", ""),
                    },
                    "side": trade.get("side", "").lower() or "unknown",
                }
                converted_trades.append(converted_trade)
            
            print(f"✓ Successfully fetched {len(converted_trades)} trades via REST API fallback")
            return converted_trades
            
        except Exception as e:
            print(f"Error: REST API fallback also failed: {e}")
            return []

    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()

    async def __aenter__(self) -> AsyncPolymarketAPIClient:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Async context manager exit."""
        await self.close()

