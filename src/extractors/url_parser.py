"""URL parsing utilities for Polymarket event URLs."""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from beartype import beartype


@beartype
def parse_event_url(url: str) -> str:
    """
    Extract event ID (tid) from a Polymarket event URL.
    
    Args:
        url: Polymarket event URL (e.g., https://polymarket.com/event/event-slug?tid=12345)
    
    Returns:
        Event ID (tid) from query parameters
    
    Raises:
        ValueError: If the URL format is invalid or doesn't contain a tid parameter
    """
    if not url:
        raise ValueError("URL must be a non-empty string")
    
    # Parse the URL
    parsed = urlparse(url)
    
    # Check if it's a Polymarket URL (polymarket.com domain)
    hostname = parsed.netloc.lower()
    if "polymarket.com" not in hostname:
        raise ValueError(f"Invalid Polymarket URL: {url}")
    
    # Check if path contains 'event'
    path = parsed.path.strip("/")
    if "event" not in path.lower():
        raise ValueError(f"URL does not contain '/event/' path: {url}")
    
    # Extract tid from query parameters
    query_params = parse_qs(parsed.query)
    tid_values = query_params.get("tid", [])
    
    if not tid_values:
        raise ValueError(f"URL does not contain 'tid' query parameter: {url}")
    
    event_id = tid_values[0]
    if not event_id:
        raise ValueError(f"Event ID (tid) is empty: {url}")
    
    return event_id

