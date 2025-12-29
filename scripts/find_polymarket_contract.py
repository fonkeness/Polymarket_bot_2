"""Utility script to find Polymarket CLOB contract address from transaction."""

from __future__ import annotations

import sys
from pathlib import Path

from beartype import beartype

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.parser.blockchain_client import PolygonBlockchainClient
from src.utils.config import POLYMARKET_CLOB_CONTRACT_ADDRESS
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Known Polymarket contract addresses (to verify)
KNOWN_POLYMARKET_CONTRACTS = [
    "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bc8B1b5c3",  # Example - needs verification
    # Add more known addresses here
]


@beartype
def find_contract_from_transaction(tx_hash: str) -> str | None:
    """
    Find contract address from a transaction hash.

    Args:
        tx_hash: Transaction hash (with or without 0x prefix)

    Returns:
        Contract address if found, None otherwise
    """
    # Normalize tx hash
    if not tx_hash.startswith("0x"):
        tx_hash = "0x" + tx_hash

    logger.info(f"Analyzing transaction: {tx_hash}")

    with PolygonBlockchainClient() as client:
        try:
            # Get transaction receipt
            receipt = client.get_transaction_receipt(tx_hash)
            
            # Get transaction details
            tx = client.get_transaction(tx_hash)
            
            logger.info(f"Transaction found in block: {receipt.get('blockNumber')}")
            logger.info(f"From: {tx.get('from')}")
            logger.info(f"To: {tx.get('to')}")
            
            # Check if this is a contract creation transaction
            if tx.get("to") is None:
                # Contract creation - get contract address from receipt
                contract_address = receipt.get("contractAddress")
                if contract_address:
                    logger.info(f"Contract created at: {contract_address}")
                    return contract_address
            
            # Otherwise, check if 'to' address is a known contract
            to_address = tx.get("to")
            if to_address:
                logger.info(f"Transaction to contract: {to_address}")
                
                # Check logs for contract interactions
                logs = receipt.get("logs", [])
                logger.info(f"Found {len(logs)} log entries")
                
                # Look for OrderFilled or similar events
                for i, log in enumerate(logs):
                    log_address = log.get("address")
                    topics = log.get("topics", [])
                    logger.debug(f"Log {i}: address={log_address}, topics={len(topics)}")
                
                return to_address
            
            return None

        except Exception as e:
            logger.error(f"Error analyzing transaction: {e}")
            return None


@beartype
def verify_contract_address(contract_address: str) -> bool:
    """
    Verify that an address is a valid Polymarket CLOB contract.

    Args:
        contract_address: Contract address to verify

    Returns:
        True if contract appears to be valid, False otherwise
    """
    logger.info(f"Verifying contract: {contract_address}")

    with PolygonBlockchainClient() as client:
        try:
            # Check if address has code (is a contract)
            code = client.web3.eth.get_code(contract_address)
            if not code or code == "0x":
                logger.warning(f"Address {contract_address} has no code (not a contract)")
                return False

            logger.info(f"Contract has code (length: {len(code) // 2 - 1} bytes)")

            # Try to get events from recent blocks to verify it's Polymarket
            current_block = client.get_current_block_number()
            from_block = max(0, current_block - 1000)  # Last 1000 blocks
            
            # Try to find OrderFilled events
            events = client.get_events(
                contract_address=contract_address,
                event_signature="OrderFilled(address,bytes32,int256,int256,address,uint256)",
                from_block=from_block,
                to_block=current_block,
            )

            logger.info(f"Found {len(events)} OrderFilled events in recent blocks")
            
            if len(events) > 0:
                logger.info("Contract appears to be Polymarket CLOB (found OrderFilled events)")
                return True
            else:
                logger.warning("No OrderFilled events found - contract may not be Polymarket CLOB")
                return False

        except Exception as e:
            logger.error(f"Error verifying contract: {e}")
            return False


@beartype
def main(tx_hash: str | None = None) -> None:
    """
    Main function to find and verify Polymarket contract.

    Args:
        tx_hash: Optional transaction hash to analyze
    """
    print("Polymarket Contract Finder")
    print("=" * 50)

    # If tx_hash provided, use it
    if tx_hash:
        contract_address = find_contract_from_transaction(tx_hash)
        if contract_address:
            print(f"\nFound contract address: {contract_address}")
            if verify_contract_address(contract_address):
                print(f"\n✓ Contract verified as Polymarket CLOB")
                print(f"\nAdd this to src/utils/config.py:")
                print(f'POLYMARKET_CLOB_CONTRACT_ADDRESS = "{contract_address}"')
            else:
                print(f"\n✗ Contract verification failed")
        else:
            print(f"\n✗ Could not find contract address from transaction")
        return

    # Otherwise, check known contracts or prompt for tx hash
    if POLYMARKET_CLOB_CONTRACT_ADDRESS:
        print(f"Current config address: {POLYMARKET_CLOB_CONTRACT_ADDRESS}")
        if verify_contract_address(POLYMARKET_CLOB_CONTRACT_ADDRESS):
            print("✓ Current address is valid")
        else:
            print("✗ Current address verification failed")
    else:
        print("No contract address configured")
        print("\nTo find contract from transaction, run:")
        print(f"  python scripts/find_polymarket_contract.py <tx_hash>")
        print("\nExample:")
        print("  python scripts/find_polymarket_contract.py 0xa08bf5e2acca8cf3f85209795cf278128a30869c5a8bfb97851e19fd21d0d21e")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        tx_hash = sys.argv[1]
        main(tx_hash)
    else:
        main()

