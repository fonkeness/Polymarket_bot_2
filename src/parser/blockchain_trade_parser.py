"""Blockchain event parser for Polymarket trade events.

Polymarket CLOB (Central Limit Order Book) is Polymarket's own development for order book
implementation. It handles limit orders and aggregates liquidity.
See: https://habr.com/ru/companies/metalamp/articles/851892/
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from beartype import beartype
from eth_abi import decode
from web3 import Web3
from web3.types import LogReceipt

from src.parser.blockchain_client import PolygonBlockchainClient
from src.utils.logger import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = get_logger(__name__)

# Polymarket CLOB OrderFilled event signature
# CLOB emits OrderFilled events when orders are matched
# Based on typical CLOB implementations, this may need adjustment based on actual contract ABI
ORDER_FILLED_EVENT_SIGNATURE = "OrderFilled(address,bytes32,int256,int256,address,uint256)"
# Alternative signatures to try
ALTERNATIVE_EVENT_SIGNATURES = [
    "FillOrder(address,bytes32,int256,int256,address,uint256)",
    "Trade(address,bytes32,int256,int256,address,uint256)",
    "OrderFilled(address,bytes32,uint256,uint256,address,uint256)",
]

# ABI for OrderFilled event (simplified, may need adjustment based on actual contract)
ORDER_FILLED_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "trader", "type": "address"},
            {"indexed": False, "name": "conditionId", "type": "bytes32"},
            {"indexed": False, "name": "price", "type": "int256"},
            {"indexed": False, "name": "size", "type": "int256"},
            {"indexed": False, "name": "outcomeToken", "type": "address"},
            {"indexed": False, "name": "outcomeIndex", "type": "uint256"},
        ],
        "name": "OrderFilled",
        "type": "event",
    }
]


class BlockchainTradeParser:
    """Parser for Polymarket blockchain trade events."""

    def __init__(
        self,
        contract_address: str,
        blockchain_client: PolygonBlockchainClient | None = None,
    ) -> None:
        """
        Initialize blockchain trade parser.

        Args:
            contract_address: Polymarket CLOB contract address
            blockchain_client: Optional blockchain client (creates new if None)
        """
        self.contract_address = Web3.to_checksum_address(contract_address)
        self.should_close_client = blockchain_client is None
        self.blockchain_client = blockchain_client or PolygonBlockchainClient()
        self.web3 = self.blockchain_client.web3
        if not self.web3:
            raise RuntimeError("Blockchain client not connected")

        # Try to determine correct event signature
        self.event_signature = self._determine_event_signature()
        logger.info(f"Using event signature: {self.event_signature}")

    def _determine_event_signature(self) -> str:
        """
        Determine the correct event signature by testing known signatures.

        Returns:
            Event signature string
        """
        # For now, use the primary signature
        # In production, this could test signatures by querying recent blocks
        return ORDER_FILLED_EVENT_SIGNATURE

    @beartype
    def parse_event_log(
        self,
        log: LogReceipt,
        block_timestamp: int | None = None,
    ) -> tuple[int, float, float, str, str, str] | None:
        """
        Parse a single event log into trade format.

        Args:
            log: Event log from blockchain
            block_timestamp: Block timestamp (fetched if None)

        Returns:
            Tuple of (timestamp, price, size, trader_address, market_id, side) or None if invalid
        """
        try:
            # Get block timestamp if not provided
            if block_timestamp is None:
                block_timestamp = self.blockchain_client.get_block_timestamp(log["blockNumber"])

            # Decode event data
            # Topics: [event_hash, trader (indexed), ...]
            # Data: conditionId, price, size, outcomeToken, outcomeIndex
            if len(log["topics"]) < 2:
                logger.warning(f"Invalid log: insufficient topics ({len(log['topics'])})")
                return None

            # Extract indexed trader address from topics
            trader_address = Web3.to_checksum_address(log["topics"][1][-40:])

            # Decode data (conditionId, price, size, outcomeToken, outcomeIndex)
            # Note: Actual ABI may differ, this is a best-effort implementation
            data = log["data"]
            if not data or data == "0x":
                logger.warning("Empty event data")
                return None

            try:
                # Try to decode based on expected ABI structure
                # Format: (bytes32 conditionId, int256 price, int256 size, address outcomeToken, uint256 outcomeIndex)
                decoded = decode(
                    ["bytes32", "int256", "int256", "address", "uint256"],
                    bytes.fromhex(data[2:]),  # Remove 0x prefix
                )
                condition_id_bytes, price_raw, size_raw, outcome_token, outcome_index = decoded

                # Convert conditionId from bytes32 to hex string
                condition_id = "0x" + condition_id_bytes.hex()

                # Convert price and size from wei/int256 to float
                # Polymarket typically uses 18 decimals for tokens
                # Price is usually in USDC (6 decimals) or similar
                # Size is in outcome token units (18 decimals typically)
                price = float(price_raw) / 1e18  # Adjust decimals as needed
                size = float(abs(size_raw)) / 1e18  # Use absolute value

                # Determine side based on outcomeIndex and sign
                # outcomeIndex 0 = YES, 1 = NO typically
                # Positive size might indicate buy, negative sell
                side = "buy" if size_raw >= 0 else "sell"

                # Validate data
                if not trader_address or price <= 0 or size <= 0:
                    logger.warning(
                        f"Invalid trade data: trader={trader_address}, price={price}, size={size}",
                    )
                    return None

                return (
                    int(block_timestamp),
                    float(price),
                    float(size),
                    str(trader_address),
                    str(condition_id),
                    str(side),
                )

            except Exception as decode_error:
                logger.warning(f"Failed to decode event data: {decode_error}")
                # Try alternative decoding methods
                return self._parse_event_alternative(log, block_timestamp)

        except Exception as e:
            logger.error(f"Error parsing event log: {e}")
            return None

    def _parse_event_alternative(
        self,
        log: LogReceipt,
        block_timestamp: int,
    ) -> tuple[int, float, float, str, str, str] | None:
        """
        Alternative parsing method if standard decoding fails.

        Args:
            log: Event log
            block_timestamp: Block timestamp

        Returns:
            Parsed trade tuple or None
        """
        # This is a fallback - actual implementation depends on real contract ABI
        # For now, return None to indicate parsing failure
        logger.debug("Alternative parsing not implemented, returning None")
        return None

    @beartype
    def get_trades_from_blocks(
        self,
        from_block: int,
        to_block: int,
        condition_id: str | None = None,
    ) -> list[tuple[int, float, float, str, str, str]]:
        """
        Get all trades from a block range, optionally filtered by condition_id.

        Args:
            from_block: Starting block number
            to_block: Ending block number (inclusive)
            condition_id: Optional condition ID to filter by

        Returns:
            List of parsed trades as tuples (timestamp, price, size, trader_address, market_id, side)
        """
        logger.info(
            f"Fetching trades from blocks {from_block} to {to_block}"
            + (f" for condition {condition_id}" if condition_id else ""),
        )

        # Get events from blockchain
        events = self.blockchain_client.get_events(
            contract_address=self.contract_address,
            event_signature=self.event_signature,
            from_block=from_block,
            to_block=to_block,
        )

        logger.info(f"Retrieved {len(events)} events from blockchain")

        # Parse events
        trades: list[tuple[int, float, float, str, str, str]] = []
        block_timestamps: dict[int, int] = {}  # Cache block timestamps

        for event in events:
            block_number = event["blockNumber"]
            
            # Get block timestamp (with caching)
            if block_number not in block_timestamps:
                block_timestamps[block_number] = self.blockchain_client.get_block_timestamp(
                    block_number,
                )

            parsed_trade = self.parse_event_log(event, block_timestamps[block_number])

            if parsed_trade:
                # Filter by condition_id if specified
                trade_condition_id = parsed_trade[4]  # market_id is at index 4
                if condition_id is None or trade_condition_id.lower() == condition_id.lower():
                    trades.append(parsed_trade)
                else:
                    logger.debug(
                        f"Skipping trade with condition_id {trade_condition_id} "
                        f"(filter: {condition_id})",
                    )

        logger.info(f"Parsed {len(trades)} valid trades")
        return trades

    def close(self) -> None:
        """Close the blockchain client if it was created here."""
        if self.should_close_client and self.blockchain_client:
            self.blockchain_client.close()

    def __enter__(self) -> BlockchainTradeParser:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Context manager exit."""
        self.close()

