"""Test script to check if API supports timestamp-based pagination parameters."""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.parser.api_client import AsyncPolymarketAPIClient
import httpx


async def test_timestamp_params():
    """Test if API supports before/after/timestamp parameters."""
    print("=" * 80)
    print("Testing timestamp-based pagination parameters")
    print("=" * 80)
    
    condition_id = "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01"
    
    # First, get a reference timestamp from a normal request
    api_client = AsyncPolymarketAPIClient()
    try:
        # Get first batch to find a reference timestamp
        trades = await api_client.get_trades(condition_id, limit=10, offset=0)
        if not trades:
            print("No trades found. Cannot test.")
            return
        
        reference_timestamp = trades[-1].get("timestamp")  # Oldest trade in first batch
        if not reference_timestamp:
            print("No timestamp found in trades. Cannot test.")
            return
        
        ref_date = datetime.fromtimestamp(reference_timestamp).strftime("%Y-%m-%d %H:%M:%S")
        print(f"\nReference timestamp: {reference_timestamp} ({ref_date})")
        print(f"Testing different parameter combinations...\n")
    finally:
        await api_client.close()
    
    # Test different parameter combinations
    test_params = [
        # Test 1: before parameter
        {"market": condition_id, "limit": 10, "before": reference_timestamp},
        {"market": condition_id, "limit": 10, "before": str(reference_timestamp)},
        
        # Test 2: after parameter
        {"market": condition_id, "limit": 10, "after": reference_timestamp},
        {"market": condition_id, "limit": 10, "after": str(reference_timestamp)},
        
        # Test 3: timestamp parameter
        {"market": condition_id, "limit": 10, "timestamp": reference_timestamp},
        {"market": condition_id, "limit": 10, "timestamp": str(reference_timestamp)},
        
        # Test 4: timestamp_lt / timestamp_gt
        {"market": condition_id, "limit": 10, "timestamp_lt": reference_timestamp},
        {"market": condition_id, "limit": 10, "timestamp_gt": reference_timestamp},
        
        # Test 5: start_time / end_time
        {"market": condition_id, "limit": 10, "start_time": reference_timestamp},
        {"market": condition_id, "limit": 10, "end_time": reference_timestamp},
        
        # Test 6: from / to
        {"market": condition_id, "limit": 10, "from": reference_timestamp},
        {"market": condition_id, "limit": 10, "to": reference_timestamp},
        
        # Test 7: since parameter
        {"market": condition_id, "limit": 10, "since": reference_timestamp},
    ]
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        url = "https://data-api.polymarket.com/trades"
        
        for i, params in enumerate(test_params, 1):
            param_name = list(params.keys())[2] if len(params) > 2 else "unknown"
            print(f"Test {i:2d}: Parameter '{param_name}' = {params.get(param_name)}", end=" ... ")
            
            try:
                response = await client.get(url, params=params)
                
                if response.status_code == 200:
                    trades = response.json()
                    if isinstance(trades, list):
                        print(f"✓ OK - Got {len(trades)} trades")
                        if trades:
                            first_ts = trades[0].get("timestamp")
                            last_ts = trades[-1].get("timestamp")
                            if first_ts and last_ts:
                                first_date = datetime.fromtimestamp(first_ts).strftime("%Y-%m-%d %H:%M:%S")
                                last_date = datetime.fromtimestamp(last_ts).strftime("%Y-%m-%d %H:%M:%S")
                                print(f"      Range: {last_date} to {first_date}")
                                
                                # Check if we got different data (older trades)
                                if last_ts < reference_timestamp:
                                    print(f"      ⭐ SUCCESS: Got older trades! ({last_date} < {ref_date})")
                        else:
                            print("      (empty response)")
                    else:
                        print(f"✗ Unexpected response format: {type(trades)}")
                elif response.status_code == 400:
                    print(f"✗ Bad Request (400) - Parameter not supported or invalid")
                elif response.status_code == 422:
                    print(f"✗ Unprocessable Entity (422) - Parameter not supported")
                else:
                    print(f"✗ Error {response.status_code}: {response.text[:100]}")
                    
            except Exception as e:
                print(f"✗ Exception: {type(e).__name__}: {e}")
            
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.2)
    
    print("\n" + "=" * 80)
    print("Testing complete!")
    print("=" * 80)
    print("\nIf any test showed '⭐ SUCCESS', that parameter works for timestamp-based pagination.")
    print("If all tests failed, API does not support timestamp-based pagination.")


async def test_alternative_approach():
    """Test if we can use offset with smaller limit to work around the issue."""
    print("\n" + "=" * 80)
    print("Testing alternative: Smaller limit to work around offset issue")
    print("=" * 80)
    
    condition_id = "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01"
    api_client = AsyncPolymarketAPIClient()
    
    try:
        # Test with smaller limits
        for limit in [100, 50, 25]:
            print(f"\nTesting with limit={limit}:")
            all_timestamps = set()
            offset = 0
            max_iterations = 30
            
            for i in range(max_iterations):
                trades = await api_client.get_trades(condition_id, limit=limit, offset=offset)
                if not trades:
                    print(f"  Stopped at offset {offset}: No more trades")
                    break
                
                batch_timestamps = {t.get("timestamp") for t in trades if t.get("timestamp")}
                duplicates = batch_timestamps.intersection(all_timestamps)
                
                if duplicates:
                    print(f"  ⚠️  Offset {offset}: Found {len(duplicates)} duplicate timestamps - stopping")
                    break
                
                all_timestamps.update(batch_timestamps)
                
                if i % 5 == 0:
                    print(f"  Offset {offset:>6}: Got {len(trades)} trades | Unique: {len(all_timestamps):>6}")
                
                offset += len(trades)
                
                if len(trades) < limit:
                    print(f"  Stopped at offset {offset}: Got fewer than requested")
                    break
            
            print(f"  Total unique timestamps with limit={limit}: {len(all_timestamps):,}")
            
            if len(all_timestamps) > 2000:
                print(f"  ✓ SUCCESS: Got more than 2000 unique trades with limit={limit}!")
                break
            
            await asyncio.sleep(0.5)  # Rate limiting
            
    finally:
        await api_client.close()


async def main():
    """Run all tests."""
    await test_timestamp_params()
    await test_alternative_approach()


if __name__ == "__main__":
    asyncio.run(main())

