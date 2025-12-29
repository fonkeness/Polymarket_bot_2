"""Configuration constants for the Polymarket Bot."""

from __future__ import annotations

from pathlib import Path

# Database configuration
DB_DIR = Path(__file__).parent.parent.parent / "data"
DB_DIR.mkdir(exist_ok=True)
DB_PATH = DB_DIR / "trades.db"

# API configuration
POLYMARKET_API_BASE_URL = "https://clob.polymarket.com"
POLYMARKET_API_V1_URL = f"{POLYMARKET_API_BASE_URL}/v1"
POLYMARKET_GAMMA_API_URL = "https://gamma-api.polymarket.com"
POLYMARKET_DATA_API_URL = "https://data-api.polymarket.com"

# The Graph API configuration
THE_GRAPH_API_KEY = "839805a5fb864b40b2fa49bca0a4c38d"
THE_GRAPH_SUBGRAPH_ID = "Bx1W4S7kDVxs9gC3s2G6DS8kdNBJNVhMviCtin2DiBp"
THE_GRAPH_API_URL = f"https://gateway.thegraph.com/api/{THE_GRAPH_API_KEY}/subgraphs/id/{THE_GRAPH_SUBGRAPH_ID}"

# Trade fetching limits
INITIAL_TRADE_LIMIT = 500

# API rate limiting (requests per second)
API_RATE_LIMIT = 10.0  # Conservative default

# Async configuration
ASYNC_CONCURRENT_REQUESTS = 5  # Number of parallel requests
ASYNC_BATCH_SIZE = 1000  # Batch size for DB saving

