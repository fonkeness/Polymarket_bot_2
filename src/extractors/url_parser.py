"""URL parsing utilities for Polymarket event URLs."""

from __future__ import annotations

from urllib.parse import urlparse

from beartype import beartype


@beartype
def parse_event_url(url: str) -> str:
    """
    Extract event slug from a Polymarket event URL.

    Args:
        url: Polymarket event URL (e.g., https://polymarket.com/event/event-slug-name)

    Returns:
        Event slug/identifier extracted from the URL

    Raises:
        ValueError: If the URL format is invalid or doesn't contain an event slug
    """
    if not url or not isinstance(url, str):
        raise ValueError("URL must be a non-empty string")

    # Parse the URL
    parsed = urlparse(url)

    # Check if it's a Polymarket URL (polymarket.com domain)
    hostname = parsed.netloc.lower()
    if "polymarket.com" not in hostname:
        raise ValueError(f"Invalid Polymarket URL: {url}")

    # Extract path and split into segments
    path = parsed.path.strip("/")
    if not path:
        raise ValueError(f"URL does not contain an event path: {url}")

    path_segments = path.split("/")

    # Look for 'event' in the path and get the next segment
    try:
        event_index = path_segments.index("event")
        if event_index + 1 >= len(path_segments):
            raise ValueError(f"URL does not contain an event slug: {url}")

        event_slug = path_segments[event_index + 1]

        # Validate event slug is not empty
        if not event_slug:
            raise ValueError(f"Event slug is empty: {url}")

        return event_slug
    except ValueError as e:
        # Check if it's our ValueError or from index()
        if "URL does not contain" in str(e) or "Event slug is empty" in str(e):
            raise
        # index() raised ValueError because 'event' not found
        raise ValueError(f"URL does not contain '/event/' path: {url}") from e

