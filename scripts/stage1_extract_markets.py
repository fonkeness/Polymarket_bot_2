"""Main entry point for Stage 1: Market Data Extraction."""

from __future__ import annotations

import sys
from pathlib import Path

from beartype import beartype

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extractors.market_extractor import extract_markets


@beartype
def main(event_url: str) -> None:
    """
    Main function to extract markets from a Polymarket event URL.

    Args:
        event_url: Polymarket event URL
    """
    print("Stage 1: Market Data Extraction")
    print(f"Event URL: {event_url}")
    print("-" * 50)

    try:
        # Extract markets
        print("Extracting markets from event URL...")
        markets = extract_markets(event_url)

        if not markets:
            print("No markets found for this event.")
            return

        print(f"\nFound {len(markets)} market(s):\n")

        # Print formatted output
        for i, market in enumerate(markets, 1):
            print(f"{i}. Market ID: {market.id}")
            print(f"   Name: {market.name}\n")

        print("Stage 1 completed successfully!")
    except ValueError as e:
        print(f"\nError: Invalid URL format - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/stage1_extract_markets.py <event_url>")
        print("Example: python scripts/stage1_extract_markets.py https://polymarket.com/event/event-slug")
        sys.exit(1)

    event_url = sys.argv[1]
    main(event_url)

