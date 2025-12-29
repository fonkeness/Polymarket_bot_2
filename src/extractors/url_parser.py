"""URL parsing utilities for Polymarket event URLs."""

from __future__ import annotations

import re
from urllib.parse import urlparse

from beartype import beartype


@beartype
def parse_event_url(url: str) -> str:
    """
    Extract event slug from a Polymarket event URL.
    
    Args:
        url: Polymarket event URL (e.g., https://polymarket.com/event/event-slug?tid=12345)
    
    Returns:
        Event slug from the path (e.g., "event-slug")
    
    Raises:
        ValueError: If the URL format is invalid
    """
    if not url:
        raise ValueError("URL must be a non-empty string")
    
    # Parse the URL
    parsed = urlparse(url)
    
    # Check if it's a Polymarket URL
    if "polymarket.com" not in parsed.netloc.lower():
        raise ValueError(f"Invalid Polymarket URL: {url}")
    
    # Extract slug from path: /event/slug-name -> slug-name
    match = re.search(r'/event/([^?/]+)', parsed.path)
    if not match:
        raise ValueError(f"URL does not contain '/event/' path: {url}")
    
    slug = match.group(1)
    if not slug:
        raise ValueError(f"Event slug is empty: {url}")
    
    return slug

