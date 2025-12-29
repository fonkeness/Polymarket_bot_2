"""Auto-find Polymarket CLOB contract by scanning recent blocks."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.parser.blockchain_client import PolygonBlockchainClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


def find_clob_contract(max_blocks: int = 1000) -> None:
    """Find CLOB contract by scanning recent blocks."""
    print("=" * 60)
    print("Auto-searching for Polymarket CLOB contract...")
    print(f"Scanning last {max_blocks} blocks on Polygon")
    print("=" * 60)
    
    with PolygonBlockchainClient() as client:
        try:
            current_block = client.get_current_block_number()
            from_block = max(0, current_block - max_blocks)
            
            print(f"\nCurrent block: {current_block}")
            print(f"Scanning from block {from_block} to {current_block}")
            print("\nThis may take a few minutes...\n")
            
            # Try multiple event signatures - Polymarket might use different ones
            event_signatures = [
                "OrderFilled(address,bytes32,int256,int256,address,uint256)",
                "FillOrder(address,bytes32,int256,int256,address,uint256)",
                "OrderFilled(address,bytes32,uint256,uint256,address,uint256)",
                "Trade(address,bytes32,int256,int256,address,uint256)",
            ]
            
            # Dictionary to count events per contract
            contract_event_counts: dict[str, int] = {}
            
            # Scan blocks in chunks
            chunk_size = 50
            total_chunks = (current_block - from_block) // chunk_size + 1
            processed = 0
            
            # Try each event signature
            for event_signature in event_signatures:
                print(f"\nTrying event signature: {event_signature}")
                event_topic = client.web3.keccak(text=event_signature).hex()
                
                for chunk_start in range(from_block, current_block, chunk_size):
                    chunk_end = min(chunk_start + chunk_size - 1, current_block)
                    processed += 1
                    
                    if processed % 10 == 0:
                        progress = (processed / (total_chunks * len(event_signatures))) * 100
                        print(f"Progress: {progress:.1f}%")
                    
                    try:
                        # Get all logs with event signature
                        # Use hex format for blocks to avoid RPC errors
                        filter_params = {
                            "fromBlock": hex(chunk_start),
                            "toBlock": hex(chunk_end),
                            "topics": [event_topic],
                        }
                        
                        logs = client.web3.eth.get_logs(filter_params)
                        
                        # Count events per contract
                        for log in logs:
                            contract_addr = log["address"].lower()
                            contract_event_counts[contract_addr] = contract_event_counts.get(contract_addr, 0) + 1
                        
                    except Exception as e:
                        # Silently continue on errors (some signatures might not exist)
                        continue
                
                # Reset processed counter for next signature
                processed = 0
            
            # Sort by event count
            sorted_contracts = sorted(
                contract_event_counts.items(),
                key=lambda x: x[1],
                reverse=True,
            )
            
            print(f"\n{'='*60}")
            print(f"Found {len(sorted_contracts)} contracts with OrderFilled events")
            print(f"{'='*60}\n")
            
            if sorted_contracts:
                print("Top candidates (most active contracts):\n")
                for i, (addr, count) in enumerate(sorted_contracts[:5], 1):
                    print(f"{i}. Address: {addr}")
                    print(f"   Events: {count}")
                    
                    # Verify contract
                    try:
                        code = client.web3.eth.get_code(addr)
                        if code and code != "0x":
                            print(f"   ✓ Has contract code")
                            
                            # Try to get more events to verify
                            test_events = client.get_events(
                                contract_address=addr,
                                event_signature=event_signature,
                                from_block=max(0, current_block - 100),
                                to_block=current_block,
                            )
                            print(f"   ✓ Found {len(test_events)} events in last 100 blocks")
                            
                            if i == 1:  # Most likely candidate
                                print(f"\n{'='*60}")
                                print("🎯 RECOMMENDED CONTRACT ADDRESS:")
                                print(f"{'='*60}")
                                print(f"\n{addr}\n")
                                print("Add this to src/utils/config.py:")
                                print(f'POLYMARKET_CLOB_CONTRACT_ADDRESS = "{addr}"')
                                print(f"\n{'='*60}")
                        else:
                            print(f"   ✗ No contract code")
                    except Exception as e:
                        print(f"   ✗ Error verifying: {e}")
                    print()
            else:
                print("No contracts found. Try increasing --max-blocks")
                
        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    max_blocks = 1000  # Default to 1000 blocks for better chance of finding events
    if len(sys.argv) > 1:
        try:
            max_blocks = int(sys.argv[1])
        except ValueError:
            print(f"Invalid max_blocks: {sys.argv[1]}, using default 1000")
    
    find_clob_contract(max_blocks)

