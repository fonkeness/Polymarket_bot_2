"""Main entry point for Stage 3: Optimized Blockchain Parsing."""

from __future__ import annotations

import sys
from pathlib import Path

from beartype import beartype

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import initialize_database
from src.database.repository import get_trade_count
from src.parser.api_client import PolymarketAPIClient
from src.parser.trade_parser import fetch_trades_from_blockchain
from src.utils.config import POLYMARKET_CLOB_CONTRACT_ADDRESS
from src.utils.logger import get_logger

logger = get_logger(__name__)


@beartype
def main(
    market_id: str,
    contract_address: str | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
) -> None:
    """
    Main function to test Stage 3 blockchain parsing functionality.

    Args:
        market_id: Polymarket market ID (numeric or conditionId)
        contract_address: Optional contract address (uses config if None)
        from_block: Optional starting block number
        to_block: Optional ending block number
    """
    print("Stage 3: Optimized Blockchain Parsing")
    print("=" * 50)
    print(f"Market ID: {market_id}")

    # Check contract address
    if not contract_address:
        contract_address = POLYMARKET_CLOB_CONTRACT_ADDRESS

    if not contract_address:
        print("\nERROR: Contract address not configured!")
        print("Please either:")
        print("  1. Set POLYMARKET_CLOB_CONTRACT_ADDRESS in src/utils/config.py")
        print("  2. Use --contract-address argument")
        print("  3. Run scripts/find_polymarket_contract.py to find the address")
        sys.exit(1)

    print(f"Contract Address: {contract_address}")
    if from_block is not None:
        print(f"From Block: {from_block}")
    if to_block is not None:
        print(f"To Block: {to_block}")
    print("-" * 50)

    # Initialize database
    print("\nInitializing database...")
    initialize_database()
    print("Database initialized successfully.")

    # Get conditionId from numeric market_id if needed
    condition_id: str | None = None
    try:
        api_client = PolymarketAPIClient()
        try:
            condition_id = api_client.get_market_condition_id(market_id)
            print(f"\nGot conditionId: {condition_id}")
        except Exception as e:
            # If get_market_condition_id fails, assume market_id is already a conditionId
            print(f"\nAssuming market_id is conditionId (error getting conditionId: {e})")
            condition_id = market_id
        finally:
            api_client.close()
    except Exception as e:
        logger.warning(f"Could not get conditionId, using market_id directly: {e}")
        condition_id = market_id

    # Fetch trades from blockchain
    print(f"\nFetching all trades from blockchain for condition {condition_id}...")
    print("This may take a while depending on the block range...")
    try:
        inserted_count = fetch_trades_from_blockchain(
            condition_id=condition_id or market_id,
            contract_address=contract_address,
            from_block=from_block,
            to_block=to_block,
        )

        print(f"\n✓ Successfully inserted {inserted_count} trades into database.")

        # Verify
        total_count = get_trade_count(condition_id or market_id)
        print(f"\nTotal trades in database for market {condition_id or market_id}: {total_count}")

        print("\n✓ Stage 3 completed successfully!")
        logger.log_summary()

    except Exception as e:
        logger.exception("Error during blockchain parsing")
        print(f"\n✗ Error: {e}")
        print("\nCheck logs/blockchain_parser.log for details.")
        sys.exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Stage 3: Fetch trades from blockchain using optimized parallel processing",
    )
    parser.add_argument(
        "market_id",
        help="Polymarket market ID (numeric or conditionId)",
    )
    parser.add_argument(
        "--contract-address",
        help="Polymarket CLOB contract address (uses config if not provided)",
    )
    parser.add_argument(
        "--from-block",
        type=int,
        help="Starting block number (uses contract deployment if not provided)",
    )
    parser.add_argument(
        "--to-block",
        type=int,
        help="Ending block number (uses current block if not provided)",
    )

    args = parser.parse_args()

    main(
        market_id=args.market_id,
        contract_address=args.contract_address,
        from_block=args.from_block,
        to_block=args.to_block,
    )

