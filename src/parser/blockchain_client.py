"""Blockchain client for Polygon network interactions."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from beartype import beartype
from web3 import Web3
from web3.exceptions import BlockNotFound, TransactionNotFound
from web3.types import BlockIdentifier, FilterParams, LogReceipt

from src.utils.config import (
    BLOCKCHAIN_RETRY_ATTEMPTS,
    BLOCKCHAIN_RETRY_DELAY,
    BLOCKCHAIN_RPC_RATE_LIMIT,
    POLYGON_RPC_ENDPOINTS,
)
from src.utils.logger import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = get_logger(__name__)


class PolygonBlockchainClient:
    """Client for interacting with Polygon blockchain via RPC."""

    def __init__(
        self,
        rpc_endpoints: Sequence[str] | None = None,
        rate_limit: float = BLOCKCHAIN_RPC_RATE_LIMIT,
    ) -> None:
        """
        Initialize Polygon blockchain client.

        Args:
            rpc_endpoints: List of RPC endpoints (uses default if None)
            rate_limit: Maximum requests per second
        """
        self.rpc_endpoints = list(rpc_endpoints) if rpc_endpoints else POLYGON_RPC_ENDPOINTS.copy()
        self.rate_limit = rate_limit
        self.current_endpoint_index = 0
        self.last_request_time = 0.0
        self.web3: Web3 | None = None
        self._connect()

    def _connect(self) -> None:
        """Connect to Polygon RPC endpoint with fallback."""
        last_error: Exception | None = None

        # Try all endpoints
        for attempt in range(len(self.rpc_endpoints)):
            endpoint = self.rpc_endpoints[self.current_endpoint_index]
            try:
                logger.info(f"Connecting to Polygon RPC: {endpoint}")
                self.web3 = Web3(Web3.HTTPProvider(endpoint))
                
                # Test connection
                block_number = self.web3.eth.block_number
                logger.info(f"Connected successfully. Current block: {block_number}")
                return

            except Exception as e:
                last_error = e
                logger.warning(f"Failed to connect to {endpoint}: {e}")
                self.current_endpoint_index = (self.current_endpoint_index + 1) % len(self.rpc_endpoints)

        # If all endpoints failed, raise error
        if last_error:
            raise ConnectionError(f"Failed to connect to any Polygon RPC endpoint: {last_error}") from last_error

    def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to respect rate limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 1.0 / self.rate_limit

        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def _retry_request(
        self,
        func: Callable[..., object],
        *args: object,
        max_retries: int = BLOCKCHAIN_RETRY_ATTEMPTS,
        **kwargs: object,
    ) -> object:
        """
        Execute a request with retry logic and fallback RPC.

        Args:
            func: Function to execute
            *args: Positional arguments for func
            max_retries: Maximum number of retry attempts
            **kwargs: Keyword arguments for func

        Returns:
            Result of func execution

        Raises:
            Exception: If all retries fail
        """
        last_error: Exception | None = None
        delay = BLOCKCHAIN_RETRY_DELAY

        for attempt in range(max_retries):
            try:
                self._wait_for_rate_limit()
                return func(*args, **kwargs)

            except Exception as e:
                last_error = e
                error_str = str(e).lower()

                # Check if it's a connection error or rate limit
                is_connection_error = any(
                    keyword in error_str
                    for keyword in ["connection", "timeout", "network", "refused"]
                )
                is_rate_limit = any(
                    keyword in error_str
                    for keyword in ["rate limit", "too many requests", "429"]
                )

                # Try next RPC endpoint on connection errors
                if is_connection_error or is_rate_limit:
                    logger.warning(
                        f"RPC error (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Trying next endpoint...",
                    )
                    self.current_endpoint_index = (
                        self.current_endpoint_index + 1
                    ) % len(self.rpc_endpoints)
                    self._connect()
                    delay = BLOCKCHAIN_RETRY_DELAY  # Reset delay
                else:
                    # For other errors, use exponential backoff
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Request failed (attempt {attempt + 1}/{max_retries}): {e}. "
                            f"Retrying in {delay}s...",
                        )
                        time.sleep(delay)
                        delay *= 2  # Exponential backoff

        # All retries failed
        if last_error:
            raise last_error

        raise RuntimeError("Request failed but no error was captured")

    @beartype
    def get_current_block_number(self) -> int:
        """
        Get the current block number.

        Returns:
            Current block number
        """
        if not self.web3:
            raise RuntimeError("Not connected to RPC")

        def _get_block() -> int:
            return self.web3.eth.block_number

        return int(self._retry_request(_get_block))

    @beartype
    def get_block(self, block_identifier: BlockIdentifier, full_transactions: bool = False) -> dict:
        """
        Get block by number or hash.

        Args:
            block_identifier: Block number or hash
            full_transactions: Whether to include full transaction data

        Returns:
            Block data dictionary
        """
        if not self.web3:
            raise RuntimeError("Not connected to RPC")

        def _get_block() -> dict:
            try:
                return dict(self.web3.eth.get_block(block_identifier, full_transactions))
            except BlockNotFound as e:
                logger.error(f"Block not found: {block_identifier}")
                raise

        return self._retry_request(_get_block)

    @beartype
    def get_block_timestamp(self, block_number: int) -> int:
        """
        Get timestamp of a specific block.

        Args:
            block_number: Block number

        Returns:
            Unix timestamp
        """
        block = self.get_block(block_number)
        return int(block.get("timestamp", 0))

    @beartype
    def get_events(
        self,
        contract_address: str,
        event_signature: str,
        from_block: int,
        to_block: int,
    ) -> list[LogReceipt]:
        """
        Get events from contract in block range.

        Args:
            contract_address: Contract address
            event_signature: Event signature (e.g., "OrderFilled(address,bytes32,int256,int256)")
            from_block: Starting block number
            to_block: Ending block number (inclusive)

        Returns:
            List of event logs
        """
        if not self.web3:
            raise RuntimeError("Not connected to RPC")

        def _get_events() -> list[LogReceipt]:
            # Create filter
            event_filter_params: FilterParams = {
                "fromBlock": from_block,
                "toBlock": to_block,
                "address": Web3.to_checksum_address(contract_address),
                "topics": [self.web3.keccak(text=event_signature).hex()],
            }

            try:
                logs = self.web3.eth.get_logs(event_filter_params)
                logger.debug(
                    f"Retrieved {len(logs)} events from blocks {from_block}-{to_block}",
                )
                return list(logs)
            except Exception as e:
                logger.error(
                    f"Error getting events from blocks {from_block}-{to_block}: {e}",
                )
                raise

        return self._retry_request(_get_events)

    @beartype
    def get_transaction(self, tx_hash: str) -> dict:
        """
        Get transaction by hash.

        Args:
            tx_hash: Transaction hash

        Returns:
            Transaction data dictionary
        """
        if not self.web3:
            raise RuntimeError("Not connected to RPC")

        def _get_tx() -> dict:
            try:
                return dict(self.web3.eth.get_transaction(tx_hash))
            except TransactionNotFound as e:
                logger.error(f"Transaction not found: {tx_hash}")
                raise

        return self._retry_request(_get_tx)

    @beartype
    def get_transaction_receipt(self, tx_hash: str) -> dict:
        """
        Get transaction receipt by hash.

        Args:
            tx_hash: Transaction hash

        Returns:
            Transaction receipt dictionary
        """
        if not self.web3:
            raise RuntimeError("Not connected to RPC")

        def _get_receipt() -> dict:
            try:
                return dict(self.web3.eth.get_transaction_receipt(tx_hash))
            except TransactionNotFound as e:
                logger.error(f"Transaction receipt not found: {tx_hash}")
                raise

        return self._retry_request(_get_receipt)

    def close(self) -> None:
        """Close the connection (no-op for HTTP provider, but kept for compatibility)."""
        logger.debug("Closing blockchain client connection")
        # HTTP provider doesn't need explicit closing, but we log it

    def __enter__(self) -> PolygonBlockchainClient:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Context manager exit."""
        self.close()

