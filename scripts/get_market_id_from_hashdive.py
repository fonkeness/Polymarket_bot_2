"""Helper script to extract market ID from hashdive.com URL or market name."""

from __future__ import annotations

import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from beartype import beartype

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extractors.market_extractor import extract_markets
from src.parser.api_client import PolymarketAPIClient


@beartype
def extract_market_from_hashdive_url(url: str) -> str | None:
    """
    Try to extract market identifier from hashdive URL.
    
    Args:
        url: hashdive.com URL
        
    Returns:
        Market name/question if found, None otherwise
    """
    parsed = urlparse(url)
    if "hashdive.com" not in parsed.netloc.lower():
        return None
    
    # Try to get market parameter from query string
    query_params = parse_qs(parsed.query)
    market_param = query_params.get("market", [None])[0]
    
    if market_param:
        # URL decode if needed
        from urllib.parse import unquote
        return unquote(market_param)
    
    return None


@beartype
def find_market_id_by_name(market_name: str, api_client: PolymarketAPIClient | None = None) -> list[tuple[str, str]]:
    """
    Search for market ID by market name/question.
    
    Args:
        market_name: Market name or question to search for
        api_client: Optional API client
        
    Returns:
        List of tuples (market_id, market_name) matching the search
    """
    # Note: Polymarket API doesn't have a direct search endpoint
    # This would require iterating through events/markets
    # For now, return empty list - user needs to provide market ID directly
    return []


@beartype
def main(url_or_market_name: str) -> None:
    """
    Main function to help find market ID.
    
    Args:
        url_or_market_name: hashdive URL or market name
    """
    print("=" * 60)
    print("Market ID Helper for Hashdive")
    print("=" * 60)
    print(f"Input: {url_or_market_name}")
    print("-" * 60)
    
    # Check if it's a URL
    if url_or_market_name.startswith("http"):
        market_name = extract_market_from_hashdive_url(url_or_market_name)
        if market_name:
            print(f"\n✓ Extracted market name from URL:")
            print(f"  {market_name}")
            print("\n" + "=" * 60)
            print("NOTE: To get the market ID, you need to:")
            print("1. Go to the actual Polymarket market page")
            print("2. Or use the browser developer tools on hashdive.com")
            print("3. Or search for the market on Polymarket website")
            print("\nOnce you have the market ID (starts with 0x...),")
            print("use: python scripts/fetch_all_trades.py <market_id>")
            return
        else:
            print("\n✗ Could not extract market name from URL")
            print("Please provide a hashdive.com URL with market parameter")
            sys.exit(1)
    else:
        print(f"\nMarket name provided: {url_or_market_name}")
        print("\n" + "=" * 60)
        print("NOTE: To find the market ID:")
        print("1. Go to https://polymarket.com")
        print("2. Search for this market")
        print("3. Open the market page")
        print("4. Check the URL or page source for market ID")
        print("\nOr use browser developer tools on hashdive.com:")
        print("1. Open the hashdive.com page")
        print("2. Press F12 to open DevTools")
        print("3. Go to Network tab")
        print("4. Look for API requests containing market ID")
        print("\nMarket IDs typically start with '0x' (hexadecimal)")
        print("Example: 0x1234567890abcdef...")
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/get_market_id_from_hashdive.py <hashdive_url_or_market_name>")
        print("\nExamples:")
        print("  python scripts/get_market_id_from_hashdive.py 'https://hashdive.com/Analyze_Market?market=Epstein+client+list...'")
        print("  python scripts/get_market_id_from_hashdive.py 'Epstein client list released in 2025?'")
        sys.exit(1)
    
    input_value = sys.argv[1]
    main(input_value)

