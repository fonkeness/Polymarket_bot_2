"""Market extraction module for Polymarket event URLs."""

from __future__ import annotations

from src.extractors.market_extractor import extract_markets
from src.extractors.models import Market
from src.extractors.url_parser import parse_event_url

__all__ = ["Market", "extract_markets", "parse_event_url"]

