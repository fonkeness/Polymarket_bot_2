"""Simple test script to check if API returns market start date."""

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.parser.api_client import AsyncPolymarketAPIClient, PolymarketAPIClient


async def test_market_start_date():
    """Test if we can get market start date from API."""
    print("=" * 80)
    print("Testing Market Start Date from Polymarket API")
    print("=" * 80)
    
    # Known event and market
    event_slug = "epstein-client-list-released-in-2025-372"
    condition_id = "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01"
    
    print(f"\nEvent: {event_slug}")
    print(f"Condition ID: {condition_id}")
    
    # Step 1: Get numeric ID from event
    print("\n" + "-" * 80)
    print("Step 1: Getting numeric market ID from event...")
    print("-" * 80)
    
    sync_client = PolymarketAPIClient()
    try:
        event_data = sync_client.get_event_markets(event_slug)
        markets = event_data.get("markets", [])
        
        if not markets:
            print("✗ No markets found in event")
            return
        
        print(f"✓ Found {len(markets)} markets in event")
        
        # Find market with matching condition_id
        target_market = None
        for market in markets:
            if market.get("conditionId") == condition_id:
                target_market = market
                break
        
        if not target_market:
            print(f"✗ Market with condition_id {condition_id} not found")
            print("Available conditionIds:")
            for m in markets[:3]:
                print(f"  - {m.get('conditionId')}")
            return
        
        numeric_id = target_market.get("id")
        print(f"✓ Found numeric ID: {numeric_id}")
        print(f"  Market name: {target_market.get('question', 'N/A')}")
        
        # Check if date fields are in event market data
        print("\n" + "-" * 80)
        print("Checking date fields in event market data...")
        print("-" * 80)
        date_fields = ["createdAt", "created_at", "startDate", "start_date", "created"]
        found_in_event = {}
        for field in date_fields:
            if field in target_market:
                found_in_event[field] = target_market[field]
        
        if found_in_event:
            print("✓ Found date fields in event market data:")
            for field, value in found_in_event.items():
                print(f"  - {field}: {value} (type: {type(value).__name__})")
        else:
            print("⚠️  No date fields found in event market data")
        
        # Step 2: Get market info using numeric ID
        print("\n" + "-" * 80)
        print(f"Step 2: Getting market info using numeric ID: {numeric_id}")
        print("-" * 80)
        
        async with AsyncPolymarketAPIClient() as async_client:
            try:
                market_info = await async_client.get_market_info(str(numeric_id))
                
                print("✓ Successfully retrieved market info")
                print(f"\nAll fields in market info:")
                for key, value in sorted(market_info.items()):
                    value_str = str(value)
                    if len(value_str) > 100:
                        value_str = value_str[:100] + "..."
                    print(f"  - {key}: {value_str} (type: {type(value).__name__})")
                
                # Check for date fields
                print("\n" + "-" * 80)
                print("Checking date-related fields in market info...")
                print("-" * 80)
                
                found_fields = {}
                for field in date_fields:
                    if field in market_info:
                        found_fields[field] = market_info[field]
                
                # Also check for any field containing date/time keywords
                date_keywords = ["date", "time", "created", "start"]
                other_date_fields = {}
                for key, value in market_info.items():
                    if any(keyword in key.lower() for keyword in date_keywords):
                        if key not in found_fields:
                            other_date_fields[key] = value
                
                if found_fields:
                    print("\n✓ Found standard date fields:")
                    for field, value in found_fields.items():
                        print(f"  - {field}: {value} (type: {type(value).__name__})")
                        
                        # Try to parse
                        if isinstance(value, (int, float)):
                            ts = int(value)
                            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                            print(f"    → Parsed as: {dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                        elif isinstance(value, str):
                            try:
                                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                                print(f"    → Parsed as: {dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                            except ValueError:
                                print(f"    → Could not parse as ISO date")
                
                if other_date_fields:
                    print("\n✓ Found other date-related fields:")
                    for field, value in other_date_fields.items():
                        print(f"  - {field}: {value} (type: {type(value).__name__})")
                
                if not found_fields and not other_date_fields:
                    print("\n⚠️  No date fields found in market info response")
                    print("   API does not return market start date")
                
                # Summary
                print("\n" + "=" * 80)
                print("SUMMARY")
                print("=" * 80)
                if found_fields:
                    print("✓ API RETURNS market start date!")
                    print(f"  Fields: {', '.join(found_fields.keys())}")
                elif other_date_fields:
                    print("⚠️  API returns date-related fields, but not standard ones")
                    print(f"  Fields: {', '.join(other_date_fields.keys())}")
                else:
                    print("✗ API DOES NOT return market start date")
                    print("  Need to use fallback strategy (oldest trade in DB - 1 day)")
                
            except Exception as e:
                print(f"✗ Error getting market info: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()
    
    finally:
        sync_client.close()


if __name__ == "__main__":
    asyncio.run(test_market_start_date())

