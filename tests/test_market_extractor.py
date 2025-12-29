"""Tests for market extractor module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.extractors.market_extractor import extract_markets
from src.extractors.models import Market
from src.parser.api_client import PolymarketAPIClient


def test_extract_markets_success() -> None:
    """Test successful market extraction."""
    event_url = "https://polymarket.com/event/test-event"
    mock_response = {
        "data": [
            {"id": "market1", "name": "Market 1"},
            {"id": "market2", "name": "Market 2"},
            {"id": "market3", "title": "Market 3"},  # Test alternative field name
        ]
    }

    with patch("src.extractors.market_extractor.parse_event_url") as mock_parse, patch(
        "src.extractors.market_extractor.PolymarketAPIClient"
    ) as mock_client_class:
        mock_parse.return_value = "test-event"

        mock_client = MagicMock(spec=PolymarketAPIClient)
        mock_client.get_event_markets.return_value = mock_response
        mock_client_class.return_value = mock_client

        markets = extract_markets(event_url)

        assert len(markets) == 3
        assert markets[0] == Market(id="market1", name="Market 1")
        assert markets[1] == Market(id="market2", name="Market 2")
        assert markets[2] == Market(id="market3", name="Market 3")

        mock_parse.assert_called_once_with(event_url)
        mock_client.get_event_markets.assert_called_once_with("test-event")
        mock_client.close.assert_called_once()


def test_extract_markets_with_custom_api_client() -> None:
    """Test market extraction with provided API client."""
    event_url = "https://polymarket.com/event/test-event"
    mock_response = {"data": [{"id": "market1", "name": "Market 1"}]}

    mock_api_client = MagicMock(spec=PolymarketAPIClient)
    mock_api_client.get_event_markets.return_value = mock_response

    with patch("src.extractors.market_extractor.parse_event_url") as mock_parse:
        mock_parse.return_value = "test-event"

        markets = extract_markets(event_url, api_client=mock_api_client)

        assert len(markets) == 1
        assert markets[0] == Market(id="market1", name="Market 1")
        # Should not close the provided client
        mock_api_client.close.assert_not_called()


def test_extract_markets_empty_response() -> None:
    """Test market extraction with empty API response."""
    event_url = "https://polymarket.com/event/test-event"
    mock_response = {"data": []}

    with patch("src.extractors.market_extractor.parse_event_url") as mock_parse, patch(
        "src.extractors.market_extractor.PolymarketAPIClient"
    ) as mock_client_class:
        mock_parse.return_value = "test-event"

        mock_client = MagicMock(spec=PolymarketAPIClient)
        mock_client.get_event_markets.return_value = mock_response
        mock_client_class.return_value = mock_client

        markets = extract_markets(event_url)

        assert len(markets) == 0


def test_extract_markets_no_data_field() -> None:
    """Test market extraction when response has no data field."""
    event_url = "https://polymarket.com/event/test-event"
    mock_response = {}

    with patch("src.extractors.market_extractor.parse_event_url") as mock_parse, patch(
        "src.extractors.market_extractor.PolymarketAPIClient"
    ) as mock_client_class:
        mock_parse.return_value = "test-event"

        mock_client = MagicMock(spec=PolymarketAPIClient)
        mock_client.get_event_markets.return_value = mock_response
        mock_client_class.return_value = mock_client

        markets = extract_markets(event_url)

        assert len(markets) == 0


def test_extract_markets_invalid_market_data() -> None:
    """Test market extraction with invalid market entries."""
    event_url = "https://polymarket.com/event/test-event"
    mock_response = {
        "data": [
            {"id": "market1", "name": "Market 1"},  # Valid
            {"id": "market2"},  # Missing name
            {"name": "Market 3"},  # Missing id
            {},  # Empty dict
            "invalid",  # Not a dict
        ]
    }

    with patch("src.extractors.market_extractor.parse_event_url") as mock_parse, patch(
        "src.extractors.market_extractor.PolymarketAPIClient"
    ) as mock_client_class:
        mock_parse.return_value = "test-event"

        mock_client = MagicMock(spec=PolymarketAPIClient)
        mock_client.get_event_markets.return_value = mock_response
        mock_client_class.return_value = mock_client

        markets = extract_markets(event_url)

        # Should only extract the valid market
        assert len(markets) == 1
        assert markets[0] == Market(id="market1", name="Market 1")


def test_extract_markets_alternative_field_names() -> None:
    """Test market extraction with alternative field names (question, title, market_id)."""
    event_url = "https://polymarket.com/event/test-event"
    mock_response = {
        "data": [
            {"market_id": "market1", "question": "Question 1"},
            {"id": "market2", "title": "Title 2"},
            {"id": "market3", "name": "Name 3"},
        ]
    }

    with patch("src.extractors.market_extractor.parse_event_url") as mock_parse, patch(
        "src.extractors.market_extractor.PolymarketAPIClient"
    ) as mock_client_class:
        mock_parse.return_value = "test-event"

        mock_client = MagicMock(spec=PolymarketAPIClient)
        mock_client.get_event_markets.return_value = mock_response
        mock_client_class.return_value = mock_client

        markets = extract_markets(event_url)

        assert len(markets) == 3
        assert markets[0] == Market(id="market1", name="Question 1")
        assert markets[1] == Market(id="market2", name="Title 2")
        assert markets[2] == Market(id="market3", name="Name 3")


def test_extract_markets_invalid_url() -> None:
    """Test market extraction with invalid URL raises ValueError."""
    event_url = "https://example.com/event/test"

    with patch("src.extractors.market_extractor.parse_event_url") as mock_parse:
        mock_parse.side_effect = ValueError("Invalid URL format")

        with pytest.raises(ValueError, match="Invalid URL format"):
            extract_markets(event_url)


def test_extract_markets_non_list_data() -> None:
    """Test market extraction when data field is not a list."""
    event_url = "https://polymarket.com/event/test-event"
    mock_response = {"data": "not a list"}

    with patch("src.extractors.market_extractor.parse_event_url") as mock_parse, patch(
        "src.extractors.market_extractor.PolymarketAPIClient"
    ) as mock_client_class:
        mock_parse.return_value = "test-event"

        mock_client = MagicMock(spec=PolymarketAPIClient)
        mock_client.get_event_markets.return_value = mock_response
        mock_client_class.return_value = mock_client

        markets = extract_markets(event_url)

        assert len(markets) == 0


