"""Tests for URL parser module."""

from __future__ import annotations

import pytest

from src.extractors.url_parser import parse_event_url


def test_parse_event_url_valid_format() -> None:
    """Test parsing valid Polymarket event URLs."""
    url = "https://polymarket.com/event/event-slug-name"
    result = parse_event_url(url)
    assert result == "event-slug-name"


def test_parse_event_url_with_trailing_slash() -> None:
    """Test parsing URL with trailing slash."""
    url = "https://polymarket.com/event/event-slug-name/"
    result = parse_event_url(url)
    assert result == "event-slug-name"


def test_parse_event_url_with_additional_path() -> None:
    """Test parsing URL with additional path segments."""
    url = "https://polymarket.com/event/event-slug-name/some/path"
    result = parse_event_url(url)
    assert result == "event-slug-name"


def test_parse_event_url_www_subdomain() -> None:
    """Test parsing URL with www subdomain."""
    url = "https://www.polymarket.com/event/event-slug-name"
    result = parse_event_url(url)
    assert result == "event-slug-name"


def test_parse_event_url_uppercase_domain() -> None:
    """Test parsing URL with uppercase domain."""
    url = "https://POLYMARKET.COM/event/event-slug-name"
    result = parse_event_url(url)
    assert result == "event-slug-name"


def test_parse_event_url_with_query_params() -> None:
    """Test parsing URL with query parameters."""
    url = "https://polymarket.com/event/event-slug-name?foo=bar"
    result = parse_event_url(url)
    assert result == "event-slug-name"


def test_parse_event_url_with_hash() -> None:
    """Test parsing URL with hash fragment."""
    url = "https://polymarket.com/event/event-slug-name#section"
    result = parse_event_url(url)
    assert result == "event-slug-name"


def test_parse_event_url_empty_string() -> None:
    """Test parsing empty string raises ValueError."""
    with pytest.raises(ValueError, match="URL must be a non-empty string"):
        parse_event_url("")


def test_parse_event_url_invalid_domain() -> None:
    """Test parsing URL with invalid domain raises ValueError."""
    with pytest.raises(ValueError, match="Invalid Polymarket URL"):
        parse_event_url("https://example.com/event/test")


def test_parse_event_url_no_event_path() -> None:
    """Test parsing URL without /event/ path raises ValueError."""
    with pytest.raises(ValueError, match="does not contain '/event/' path"):
        parse_event_url("https://polymarket.com/markets/test")


def test_parse_event_url_no_slug() -> None:
    """Test parsing URL with /event/ but no slug raises ValueError."""
    with pytest.raises(ValueError, match="does not contain an event slug"):
        parse_event_url("https://polymarket.com/event/")


def test_parse_event_url_empty_slug() -> None:
    """Test parsing URL with empty event slug raises ValueError."""
    url = "https://polymarket.com/event/"
    # After stripping, this should be treated as no slug
    with pytest.raises(ValueError):
        parse_event_url(url)


def test_parse_event_url_complex_slug() -> None:
    """Test parsing URL with complex slug containing hyphens and numbers."""
    url = "https://polymarket.com/event/event-2024-slug-name-123"
    result = parse_event_url(url)
    assert result == "event-2024-slug-name-123"


