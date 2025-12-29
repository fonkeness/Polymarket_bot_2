"""Optimized blockchain parser with parallel block processing."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

from beartype import beartype

from src.database.repository import insert_trades_batch
from src.parser.blockchain_client import PolygonBlockchainClient
from src.parser.blockchain_trade_parser import BlockchainTradeParser
from src.utils.config import (
    BLOCKCHAIN_BATCH_SIZE,
    BLOCKCHAIN_MAX_WORKERS,
    DB_BATCH_INSERT_SIZE,
    POLYMARKET_CLOB_CONTRACT_ADDRESS,
)
from src.utils.logger import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

logger = get_logger(__name__)


class OptimizedBlockchainParser:
    """Optimized parser for high-speed blockchain data collection."""

    def __init__(
        self,
        contract_address: str | None = None,
        blockchain_client: PolygonBlockchainClient | None = None,
        max_workers: int = BLOCKCHAIN_MAX_WORKERS,
        batch_size: int = BLOCKCHAIN_BATCH_SIZE,
    ) -> None:
        """
        Initialize optimized blockchain parser.

        Args:
            contract_address: Polymarket CLOB contract address (uses config default if None)
            blockchain_client: Optional blockchain client (creates new if None)
            max_workers: Maximum number of parallel workers
            batch_size: Number of blocks per batch
        """
        self.contract_address = contract_address or POLYMARKET_CLOB_CONTRACT_ADDRESS
        if not self.contract_address:
            raise ValueError(
                "Contract address not provided and not found in config. "
                "Please set POLYMARKET_CLOB_CONTRACT_ADDRESS in config or provide it here.",
            )

        self.should_close_client = blockchain_client is None
        self.blockchain_client = blockchain_client or PolygonBlockchainClient()
        self.max_workers = max_workers
        self.batch_size = batch_size

        # Statistics
        self.stats = {
            "blocks_processed": 0,
            "events_found": 0,
            "trades_parsed": 0,
            "trades_inserted": 0,
            "errors": 0,
            "start_time": time.time(),
        }

    def _split_block_range(self, from_block: int, to_block: int) -> list[tuple[int, int]]:
        """
        Split block range into batches.

        Args:
            from_block: Starting block
            to_block: Ending block

        Returns:
            List of (start_block, end_block) tuples
        """
        batches: list[tuple[int, int]] = []
        current = from_block

        while current <= to_block:
            end = min(current + self.batch_size - 1, to_block)
            batches.append((current, end))
            current = end + 1

        return batches

    def _process_block_batch(
        self,
        from_block: int,
        to_block: int,
        condition_id: str | None = None,
    ) -> list[tuple[int, float, float, str, str, str]]:
        """
        Process a single batch of blocks.

        Args:
            from_block: Starting block
            to_block: Ending block
            condition_id: Optional condition ID filter

        Returns:
            List of parsed trades
        """
        try:
            # Create a new parser instance for this batch (thread-safe)
            parser = BlockchainTradeParser(
                contract_address=self.contract_address,
                blockchain_client=self.blockchain_client,
            )

            trades = parser.get_trades_from_blocks(
                from_block=from_block,
                to_block=to_block,
                condition_id=condition_id,
            )

            parser.close()
            return trades

        except Exception as e:
            logger.error(f"Error processing blocks {from_block}-{to_block}: {e}")
            self.stats["errors"] += 1
            return []

    @beartype
    def fetch_all_trades(
        self,
        condition_id: str | None = None,
        from_block: int | None = None,
        to_block: int | None = None,
    ) -> int:
        """
        Fetch all trades from blockchain with parallel processing.

        Args:
            condition_id: Optional condition ID to filter by
            from_block: Starting block (uses contract deployment block if None)
            to_block: Ending block (uses current block if None)

        Returns:
            Total number of trades inserted into database
        """
        logger.info("Starting optimized blockchain trade fetching")

        # Determine block range
        if to_block is None:
            to_block = self.blockchain_client.get_current_block_number()
            logger.info(f"Using current block as end: {to_block}")

        if from_block is None:
            # Try to find contract deployment block
            # For now, use a reasonable default (Polymarket CLOB was deployed around block 0)
            # In production, this should query the contract creation transaction
            from_block = 0
            logger.info(f"Using block 0 as start (contract deployment block should be determined)")

        logger.info(
            f"Processing blocks {from_block} to {to_block}"
            + (f" for condition {condition_id}" if condition_id else ""),
        )

        # Split into batches
        batches = self._split_block_range(from_block, to_block)
        total_batches = len(batches)
        logger.info(f"Split into {total_batches} batches of ~{self.batch_size} blocks each")

        # Process batches in parallel
        all_trades: list[tuple[int, float, float, str, str, str]] = []
        completed_batches = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all batch tasks
            future_to_batch = {
                executor.submit(
                    self._process_block_batch,
                    batch_from,
                    batch_to,
                    condition_id,
                ): (batch_from, batch_to)
                for batch_from, batch_to in batches
            }

            # Process completed batches
            for future in as_completed(future_to_batch):
                batch_from, batch_to = future_to_batch[future]
                try:
                    batch_trades = future.result()
                    all_trades.extend(batch_trades)
                    completed_batches += 1

                    # Update statistics
                    self.stats["blocks_processed"] += (batch_to - batch_from + 1)
                    self.stats["trades_parsed"] += len(batch_trades)

                    # Log progress
                    logger.log_progress(
                        completed_batches,
                        total_batches,
                        "batches",
                        update_interval=max(1, total_batches // 20),
                    )

                    # Batch insert to database periodically
                    if len(all_trades) >= DB_BATCH_INSERT_SIZE:
                        inserted = insert_trades_batch(all_trades[:DB_BATCH_INSERT_SIZE])
                        self.stats["trades_inserted"] += inserted
                        logger.debug(f"Inserted {inserted} trades into database")
                        all_trades = all_trades[DB_BATCH_INSERT_SIZE:]

                except Exception as e:
                    logger.error(f"Error in batch {batch_from}-{batch_to}: {e}")
                    self.stats["errors"] += 1

        # Insert remaining trades
        if all_trades:
            inserted = insert_trades_batch(all_trades)
            self.stats["trades_inserted"] += inserted
            logger.info(f"Inserted final batch of {inserted} trades")

        # Log final statistics
        elapsed_time = time.time() - self.stats["start_time"]
        self._log_statistics(elapsed_time)

        return self.stats["trades_inserted"]

    def _log_statistics(self, elapsed_time: float) -> None:
        """
        Log parsing statistics.

        Args:
            elapsed_time: Total elapsed time in seconds
        """
        stats = self.stats
        logger.info("=== Parsing Statistics ===")
        logger.info(f"Blocks processed: {stats['blocks_processed']}")
        logger.info(f"Events found: {stats['events_found']}")
        logger.info(f"Trades parsed: {stats['trades_parsed']}")
        logger.info(f"Trades inserted: {stats['trades_inserted']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info(f"Total time: {elapsed_time:.2f} seconds")

        if elapsed_time > 0:
            blocks_per_sec = stats["blocks_processed"] / elapsed_time
            trades_per_sec = stats["trades_inserted"] / elapsed_time
            logger.record_metric("blocks_per_second", blocks_per_sec)
            logger.record_metric("trades_per_second", trades_per_sec)
            logger.info(f"Performance: {blocks_per_sec:.2f} blocks/sec, {trades_per_sec:.2f} trades/sec")

        logger.info("========================")

    def close(self) -> None:
        """Close the blockchain client if it was created here."""
        if self.should_close_client and self.blockchain_client:
            self.blockchain_client.close()

    def __enter__(self) -> OptimizedBlockchainParser:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Context manager exit."""
        self.close()

