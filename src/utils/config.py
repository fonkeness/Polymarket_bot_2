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

# Trade fetching limits
INITIAL_TRADE_LIMIT = 500

# API rate limiting (requests per second)
API_RATE_LIMIT = 10.0  # Conservative default

# Blockchain configuration
# Polygon mainnet RPC endpoints (public, with fallback)
POLYGON_RPC_ENDPOINTS = [
    "https://polygon-rpc.com",  # Public RPC
    "https://rpc.ankr.com/polygon",  # Ankr public RPC
    "https://polygon.llamarpc.com",  # LlamaRPC public endpoint
]

# Polymarket CLOB contract address (will be determined via contract finder)
POLYMARKET_CLOB_CONTRACT_ADDRESS: str | None = None

# Blockchain batch processing settings
BLOCKCHAIN_BATCH_SIZE = 1000  # Number of blocks per batch
BLOCKCHAIN_MAX_WORKERS = 5  # Maximum concurrent workers for block processing
BLOCKCHAIN_RPC_RATE_LIMIT = 10.0  # Requests per second to RPC
BLOCKCHAIN_RETRY_ATTEMPTS = 3  # Number of retry attempts for failed requests
BLOCKCHAIN_RETRY_DELAY = 2.0  # Initial delay between retries (seconds)

# Database batch insert size
DB_BATCH_INSERT_SIZE = 1000  # Number of trades to insert per batch

