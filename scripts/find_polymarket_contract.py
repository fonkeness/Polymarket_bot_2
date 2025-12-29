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

# Known Polymarket CLOB contract addresses on Polygon
# CLOB (Central Limit Order Book) is Polymarket's own development for order book implementation
# Source: https://habr.com/ru/companies/metalamp/articles/851892/
KNOWN_POLYMARKET_CONTRACTS = [
    # Add verified addresses here when found
    # You can find them by:
    # 1. Searching Polygonscan for "CLOB" or "Polymarket CLOB"
    # 2. Checking Polymarket GitHub repositories
    # 3. Looking for contracts with OrderFilled events
]

# Alternative: Search for contract by checking recent transactions
# Polymarket CLOB is very active, so we can search recent blocks for OrderFilled events


@beartype
def search_contract_in_recent_blocks(max_blocks: int = 500) -> list[tuple[str, int]]:
    """
    Search for Polymarket CLOB contract by scanning recent blocks for OrderFilled events.
    
    This function scans recent blocks and looks for contracts emitting OrderFilled events.
    Returns list of (contract_address, event_count) tuples sorted by event count.
    
    Args:
        max_blocks: Maximum number of recent blocks to scan
        
    Returns:
        List of tuples (contract_address, event_count) sorted by event count (descending)
    """
    logger.info(f"Searching for Polymarket CLOB contract in last {max_blocks} blocks...")
    logger.info("This may take a few minutes...")
    
    with PolygonBlockchainClient() as client:
        try:
            current_block = client.get_current_block_number()
            from_block = max(0, current_block - max_blocks)
            
            logger.info(f"Scanning blocks {from_block} to {current_block}")
            
            # Event signature for OrderFilled
            event_signature = "OrderFilled(address,bytes32,int256,int256,address,uint256)"
            event_topic = client.web3.keccak(text=event_signature).hex()
            
            # Dictionary to count events per contract
            contract_event_counts: dict[str, int] = {}
            
            # Scan blocks in chunks
            chunk_size = 50  # Smaller chunks to avoid RPC limits
            total_chunks = (current_block - from_block) // chunk_size + 1
            processed_chunks = 0
            
            for chunk_start in range(from_block, current_block, chunk_size):
                chunk_end = min(chunk_start + chunk_size - 1, current_block)
                processed_chunks += 1
                
                try:
                    logger.log_progress(
                        processed_chunks,
                        total_chunks,
                        "chunks",
                        update_interval=max(1, total_chunks // 10),
                    )
                    
                    # Get all logs in this chunk with OrderFilled event signature
                    filter_params = {
                        "fromBlock": chunk_start,
                        "toBlock": chunk_end,
                        "topics": [event_topic],
                    }
                    
                    logs = client.web3.eth.get_logs(filter_params)
                    
                    # Count events per contract
                    for log in logs:
                        contract_addr = log["address"].lower()
                        contract_event_counts[contract_addr] = contract_event_counts.get(contract_addr, 0) + 1
                    
                except Exception as e:
                    logger.debug(f"Error searching chunk {chunk_start}-{chunk_end}: {e}")
                    continue
            
            # Sort by event count (most active first)
            sorted_contracts = sorted(
                contract_event_counts.items(),
                key=lambda x: x[1],
                reverse=True,
            )
            
            logger.info(f"Found {len(sorted_contracts)} contracts with OrderFilled events")
            
            if sorted_contracts:
                logger.info("Top contracts by event count:")
                for addr, count in sorted_contracts[:10]:  # Show top 10
                    logger.info(f"  {addr}: {count} events")
            
            return sorted_contracts
            
        except Exception as e:
            logger.error(f"Error searching recent blocks: {e}")
            return []


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
            logger.info("Note: Transaction might be on a different network (Ethereum instead of Polygon)")
            logger.info("Or the transaction hash might be incorrect")
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
        print("\nOptions to find the contract:")
        print("\n1. Auto-search in recent blocks (RECOMMENDED):")
        print("   python scripts/find_polymarket_contract.py --search")
        print("   This will scan recent Polygon blocks for OrderFilled events")
        print("\n2. From transaction hash (if you have a valid Polygon transaction):")
        print(f"   python scripts/find_polymarket_contract.py <tx_hash>")
        print("\n3. Find on Polygonscan:")
        print("   - Go to https://polygonscan.com")
        print("   - Search for 'Polymarket CLOB' or 'CLOB'")
        print("   - Look for contract with recent OrderFilled events")
        print("\n4. Check Polymarket documentation:")
        print("   - Official docs may list contract addresses")
        print("   - GitHub repositories may have addresses")
        print("\nOnce you have the contract address, add it to src/utils/config.py:")
        print('   POLYMARKET_CLOB_CONTRACT_ADDRESS = "0x..."')


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Find Polymarket CLOB contract address",
    )
    parser.add_argument(
        "tx_hash",
        nargs="?",
        help="Transaction hash to analyze (optional)",
    )
    parser.add_argument(
        "--search",
        action="store_true",
        help="Search for contract in recent blocks",
    )
    parser.add_argument(
        "--max-blocks",
        type=int,
        default=500,
        help="Maximum blocks to scan when using --search (default: 500)",
    )
    
    args = parser.parse_args()
    
    if args.search:
        # Auto-search mode
        print("Polymarket Contract Finder - Auto Search Mode")
        print("=" * 50)
        print(f"Scanning last {args.max_blocks} blocks for OrderFilled events...")
        print("This may take a few minutes...\n")
        
        contracts = search_contract_in_recent_blocks(max_blocks=args.max_blocks)
        
        if contracts:
            print(f"\n✓ Found {len(contracts)} contracts with OrderFilled events")
            print("\nTop candidates (most active contracts):")
            print("-" * 50)
            
            for i, (addr, count) in enumerate(contracts[:5], 1):
                print(f"\n{i}. Address: {addr}")
                print(f"   Events found: {count}")
                
                # Verify each candidate
                print(f"   Verifying...", end=" ")
                if verify_contract_address(addr):
                    print("✓ VALID - This appears to be Polymarket CLOB!")
                    print(f"\n🎯 RECOMMENDED ADDRESS:")
                    print(f"   {addr}")
                    print(f"\nAdd this to src/utils/config.py:")
                    print(f'POLYMARKET_CLOB_CONTRACT_ADDRESS = "{addr}"')
                    break
                else:
                    print("✗ Not verified")
        else:
            print("\n✗ No contracts found with OrderFilled events")
            print("Try increasing --max-blocks or check your RPC connection")
    
    elif args.tx_hash:
        main(args.tx_hash)
    else:
        main()

