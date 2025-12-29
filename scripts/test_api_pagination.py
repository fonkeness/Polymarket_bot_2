"""Test script to diagnose API pagination issues."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.parser.api_client import AsyncPolymarketAPIClient


async def test_1_different_offsets():
    """Test 1: Check what API returns at different offsets."""
    print("=" * 80)
    print("TEST 1: Different offsets")
    print("=" * 80)
    
    api_client = AsyncPolymarketAPIClient()
    condition_id = "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01"
    
    try:
        for offset in [0, 500, 1000, 1500, 2000, 5000, 10000]:
            trades = await api_client.get_trades(condition_id, limit=500, offset=offset)
            print(f"Offset {offset:>6}: Got {len(trades)} trades", end="")
            if trades:
                from datetime import datetime
                first_ts = trades[0].get("timestamp")
                last_ts = trades[-1].get("timestamp")
                if first_ts and last_ts:
                    first_date = datetime.fromtimestamp(first_ts).strftime("%Y-%m-%d %H:%M:%S")
                    last_date = datetime.fromtimestamp(last_ts).strftime("%Y-%m-%d %H:%M:%S")
                    print(f" | Range: {last_date} to {first_date}")
                else:
                    print()
            else:
                print(" | EMPTY!")
    finally:
        await api_client.close()


async def test_2_same_data_check():
    """Test 2: Check if API returns same data at different offsets."""
    print("\n" + "=" * 80)
    print("TEST 2: Check for duplicate data at different offsets")
    print("=" * 80)
    
    api_client = AsyncPolymarketAPIClient()
    condition_id = "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01"
    
    try:
        trades_0 = await api_client.get_trades(condition_id, limit=10, offset=0)
        trades_1000 = await api_client.get_trades(condition_id, limit=10, offset=1000)
        
        print(f"Offset 0: {len(trades_0)} trades")
        print(f"Offset 1000: {len(trades_1000)} trades")
        
        if trades_0 and trades_1000:
            # Compare first trades
            sig_0 = f"{trades_0[0].get('timestamp')}|{trades_0[0].get('price')}|{trades_0[0].get('size')}"
            sig_1000 = f"{trades_1000[0].get('timestamp')}|{trades_1000[0].get('price')}|{trades_1000[0].get('size')}"
            print(f"\nFirst trade at offset 0: {sig_0}")
            print(f"First trade at offset 1000: {sig_1000}")
            if sig_0 == sig_1000:
                print("\n⚠️  WARNING: API returns same data at different offsets!")
            else:
                print("\n✓ Different data at different offsets (good)")
    finally:
        await api_client.close()


async def test_3_error_handling():
    """Test 3: Check for HTTP errors at different offsets."""
    print("\n" + "=" * 80)
    print("TEST 3: Error handling at different offsets")
    print("=" * 80)
    
    api_client = AsyncPolymarketAPIClient()
    condition_id = "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01"
    
    try:
        for offset in [0, 1000, 5000, 10000, 50000, 100000]:
            try:
                trades = await api_client.get_trades(condition_id, limit=500, offset=offset)
                print(f"Offset {offset:>6}: OK - {len(trades)} trades")
            except Exception as e:
                print(f"Offset {offset:>6}: ERROR - {type(e).__name__}: {e}")
    finally:
        await api_client.close()


async def test_4_total_available_trades():
    """Test 4: Check total number of available trades."""
    print("\n" + "=" * 80)
    print("TEST 4: Total available trades check")
    print("=" * 80)
    
    api_client = AsyncPolymarketAPIClient()
    condition_id = "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01"
    
    try:
        all_timestamps = set()
        all_signatures = set()
        offset = 0
        max_iterations = 20
        duplicate_count = 0
        
        for i in range(max_iterations):
            trades = await api_client.get_trades(condition_id, limit=500, offset=offset)
            if not trades:
                print(f"\nStopped at offset {offset}: No more trades")
                break
            
            # Collect unique timestamps and signatures
            batch_timestamps = {t.get("timestamp") for t in trades if t.get("timestamp")}
            batch_signatures = {
                f"{t.get('timestamp')}|{t.get('price')}|{t.get('size')}|{t.get('proxyWallet', '')}"
                for t in trades
                if t.get("timestamp")
            }
            
            # Check for duplicates
            timestamp_duplicates = batch_timestamps.intersection(all_timestamps)
            signature_duplicates = batch_signatures.intersection(all_signatures)
            
            if timestamp_duplicates:
                duplicate_count += len(timestamp_duplicates)
                print(f"⚠️  Offset {offset}: Found {len(timestamp_duplicates)} duplicate timestamps!")
            
            all_timestamps.update(batch_timestamps)
            all_signatures.update(batch_signatures)
            
            print(
                f"Offset {offset:>6}: Got {len(trades)} trades | "
                f"Unique timestamps: {len(all_timestamps):>6} | "
                f"Unique signatures: {len(all_signatures):>6}"
            )
            
            offset += len(trades)
            
            if len(trades) < 500:
                print(f"\nStopped at offset {offset}: Got fewer than requested ({len(trades)} < 500)")
                break
            
            # Stop if we're getting too many duplicates
            if duplicate_count > 100:
                print(f"\n⚠️  Too many duplicates detected. Stopping.")
                break
        
        print(f"\nSummary:")
        print(f"  Total unique timestamps: {len(all_timestamps):,}")
        print(f"  Total unique signatures: {len(all_signatures):,}")
        print(f"  Total duplicates found: {duplicate_count}")
        
    finally:
        await api_client.close()


async def test_5_specific_offset_range():
    """Test 5: Check specific offset range around where it stops (around 900-1000)."""
    print("\n" + "=" * 80)
    print("TEST 5: Specific offset range (900-1500)")
    print("=" * 80)
    
    api_client = AsyncPolymarketAPIClient()
    condition_id = "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01"
    
    try:
        for offset in range(900, 1501, 100):
            trades = await api_client.get_trades(condition_id, limit=500, offset=offset)
            print(f"Offset {offset:>6}: Got {len(trades)} trades", end="")
            if trades:
                from datetime import datetime
                first_ts = trades[0].get("timestamp")
                last_ts = trades[-1].get("timestamp")
                if first_ts and last_ts:
                    first_date = datetime.fromtimestamp(first_ts).strftime("%Y-%m-%d %H:%M:%S")
                    last_date = datetime.fromtimestamp(last_ts).strftime("%Y-%m-%d %H:%M:%S")
                    print(f" | Range: {last_date} to {first_date}")
                else:
                    print()
            else:
                print(" | EMPTY!")
    finally:
        await api_client.close()


async def main():
    """Run all tests."""
    print("\n" + "=" * 80)
    print("API Pagination Diagnostic Tests")
    print("=" * 80)
    print("\nRunning tests to diagnose why script stops at ~906 trades...")
    print()
    
    await test_1_different_offsets()
    await test_2_same_data_check()
    await test_3_error_handling()
    await test_4_total_available_trades()
    await test_5_specific_offset_range()
    
    print("\n" + "=" * 80)
    print("All tests completed!")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

