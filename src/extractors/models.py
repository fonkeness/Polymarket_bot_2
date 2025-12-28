"""Data models for market extraction."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Market:
    """Represents a Polymarket market with ID and name."""

    id: str
    name: str

