"""Test script to check what fields are returned by Polymarket API for market info."""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.parser.api_client import AsyncPolymarketAPIClient


async def test_market_info_fields():
    """Test what fields are returned by get_market_info API."""
    print("=" * 80)
    print("Testing Polymarket API Market Info Fields")
    print("=" * 80)
    
    # Using a known market condition_id
    condition_id = "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01"
    event_slug = "epstein-client-list-released-in-2025-372"
    
    # First, get numeric ID from event (required for Gamma API)
    print(f"\n1. Getting numeric market ID from event...")
    print("-" * 80)
    
    from src.parser.api_client import PolymarketAPIClient
    sync_client = PolymarketAPIClient()
    
    try:
        event_data = sync_client.get_event_markets(event_slug)
        markets = event_data.get("markets", [])
        
        if not markets:
            print("✗ No markets found in event")
            return
        
        # Find market with matching condition_id
        target_market = None
        for market in markets:
            if market.get("conditionId") == condition_id:
                target_market = market
                break
        
        if not target_market:
            print(f"✗ Market with condition_id {condition_id} not found")
            return
        
        numeric_id = target_market.get("id")
        print(f"✓ Found numeric ID: {numeric_id}")
        print(f"  Market name: {target_market.get('question', 'N/A')}")
        
        # Now try to get market info using numeric ID with async client (Gamma API)
        print(f"\n2. Getting market info using numeric ID with Gamma API...")
        print("-" * 80)
        
        async with AsyncPolymarketAPIClient() as api_client:
            try:
                market_info = await api_client.get_market_info(str(numeric_id))
                print("✓ Success! Response fields:")
                print(json.dumps(market_info, indent=2, default=str))
                
                # Check for date-related fields
                print("\n" + "=" * 80)
                print("Date-related fields found:")
                print("=" * 80)
                date_fields = [
                    "createdAt", "created_at", "startDate", "start_date", 
                    "created", "startTime", "start_time", "creationDate",
                    "creation_date", "dateCreated", "date_created",
                    "createdTimestamp", "created_timestamp", "startTimestamp",
                    "start_timestamp", "createdAtTimestamp", "created_at_timestamp"
                ]
                
                found_fields = {}
                for field in date_fields:
                    if field in market_info:
                        found_fields[field] = market_info[field]
                
                if found_fields:
                    print("\n✓ Found date fields:")
                    for field, value in found_fields.items():
                        print(f"  - {field}: {value} (type: {type(value).__name__})")
                else:
                    print("\n⚠️  No standard date fields found. All fields:")
                    for key, value in market_info.items():
                        print(f"  - {key}: {value} (type: {type(value).__name__})")
                        
            except Exception as e:
                print(f"✗ Error: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()
    
    finally:
        sync_client.close()


async def test_with_numeric_id():
    """Test with a numeric market ID if we can find one."""
    print("\n" + "=" * 80)
    print("Testing with numeric market ID")
    print("=" * 80)
    
    # You'll need to provide a numeric market ID here
    # This can be obtained from the event markets endpoint
    numeric_id = None  # Replace with actual numeric ID if available
    
    if not numeric_id:
        print("⚠️  No numeric ID provided. To get one:")
        print("   1. Use get_event_markets() to get markets for an event")
        print("   2. Extract the numeric 'id' field from a market")
        print("   3. Use that ID with get_market_info()")
        return
    
    async with AsyncPolymarketAPIClient() as api_client:
        try:
            market_info = await api_client.get_market_info(str(numeric_id))
            print("✓ Success! Response fields:")
            print(json.dumps(market_info, indent=2, default=str))
        except Exception as e:
            print(f"✗ Error: {type(e).__name__}: {e}")


async def test_event_markets():
    """Test getting markets from an event to see what fields are available."""
    print("\n" + "=" * 80)
    print("Testing Event Markets Endpoint")
    print("=" * 80)
    
    event_slug = "epstein-client-list-released-in-2025-372"
    
    # Note: get_event_markets is sync, but we use async client for get_market_info
    from src.parser.api_client import PolymarketAPIClient
    sync_client = PolymarketAPIClient()
    
    try:
        event_data = sync_client.get_event_markets(event_slug)
            
            print("✓ Event data retrieved!")
            print("\nEvent structure:")
            print(json.dumps(list(event_data.keys()), indent=2))
            
            if "markets" in event_data and event_data["markets"]:
                print("\n" + "=" * 80)
                print("First market fields:")
                print("=" * 80)
                first_market = event_data["markets"][0]
                print(json.dumps(first_market, indent=2, default=str))
                
                # Check for date fields in market
                date_fields = [
                    "createdAt", "created_at", "startDate", "start_date", 
                    "created", "startTime", "start_time", "creationDate",
                    "creation_date", "dateCreated", "date_created"
                ]
                
                print("\n" + "=" * 80)
                print("Date-related fields in market:")
                print("=" * 80)
                found_fields = {}
                for field in date_fields:
                    if field in first_market:
                        found_fields[field] = first_market[field]
                
                if found_fields:
                    print("\n✓ Found date fields:")
                    for field, value in found_fields.items():
                        print(f"  - {field}: {value} (type: {type(value).__name__})")
                else:
                    print("\n⚠️  No standard date fields found in market object")
                    
                # Try to get market info using numeric ID with async client (Gamma API)
                if "id" in first_market:
                    numeric_id = first_market["id"]
                    print(f"\n" + "=" * 80)
                    print(f"Testing get_market_info with numeric ID: {numeric_id} (using Gamma API)")
                    print("=" * 80)
                    try:
                        # Use async client for Gamma API (not sync client which uses CLOB API)
                        async with AsyncPolymarketAPIClient() as async_api_client:
                            market_info = await async_api_client.get_market_info(str(numeric_id))
                            print("✓ Success! Market info fields:")
                            print(json.dumps(market_info, indent=2, default=str))
                            
                            # Check for date fields
                            print("\n" + "=" * 80)
                            print("Date-related fields in market info:")
                            print("=" * 80)
                            found_fields = {}
                            for field in date_fields:
                                if field in market_info:
                                    found_fields[field] = market_info[field]
                            
                            if found_fields:
                                print("\n✓ Found date fields:")
                                for field, value in found_fields.items():
                                    print(f"  - {field}: {value} (type: {type(value).__name__})")
                            else:
                                print("\n⚠️  No standard date fields found. All fields:")
                                for key, value in market_info.items():
                                    if any(date_word in key.lower() for date_word in ["date", "time", "created", "start"]):
                                        print(f"  - {key}: {value} (type: {type(value).__name__})")
                    except Exception as e:
                        print(f"✗ Error getting market info: {type(e).__name__}: {e}")
                        import traceback
                        traceback.print_exc()
    
    except Exception as e:
        print(f"✗ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        sync_client.close()


async def main():
    """Run all tests."""
    await test_market_info_fields()
    await test_event_markets()
    await test_with_numeric_id()
    
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print("\nTo get market start date:")
    print("1. Use Gamma API endpoint: GET /markets/{numeric_id}")
    print("2. Check response for fields like: createdAt, created_at, startDate, etc.")
    print("3. The field might be in Unix timestamp (int) or ISO format (string)")
    print("\nNote: get_market_info() requires numeric market ID, not condition_id")
    print("You can get numeric ID from: GET /events/slug/{slug} -> markets[].id")


if __name__ == "__main__":
    asyncio.run(main())

