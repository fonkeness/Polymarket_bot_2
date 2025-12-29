"""Configuration constants for the Polymarket Bot."""

from __future__ import annotations

import os
from pathlib import Path

# Database configuration
DB_DIR = Path(__file__).parent.parent.parent / "data"
DB_DIR.mkdir(exist_ok=True)
DB_PATH = DB_DIR / "trades.db"

# API configuration
POLYMARKET_API_BASE_URL = "https://clob.polymarket.com"
POLYMARKET_API_V1_URL = f"{POLYMARKET_API_BASE_URL}/v1"
POLYMARKET_GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Trade fetching limits
INITIAL_TRADE_LIMIT = 500

# API rate limiting (requests per second)
API_RATE_LIMIT = 10.0  # Conservative default

