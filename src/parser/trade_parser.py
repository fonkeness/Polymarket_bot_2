"""Trade data parsing and transformation logic."""

from __future__ import annotations

from beartype import beartype

from src.parser.api_client import PolymarketAPIClient
from src.parser.optimized_blockchain_parser import OptimizedBlockchainParser


@beartype
def parse_trade_data(trade: dict[str, object], market_id: str) -> tuple[int, float, float, str, str, str] | None:
    """
    Parse a single trade from API response to database format.
    Supports both REST API format (converted to The Graph format for compatibility).

    Args:
        trade: Trade dictionary from REST API response (converted format)
        market_id: Market conditionId to associate with this trade

    Returns:
        Tuple of (timestamp, price, size, trader_address, market_id, side) or None if invalid
    """
    try:
        timestamp = int(trade.get("timestamp", 0))
        price = float(trade.get("price", 0.0))
        
        # REST API uses "size", but converted format uses "amount"
        size = float(trade.get("amount") or trade.get("size", 0.0))
        
        # REST API uses proxyWallet, but converted format uses nested user.id
        user_obj = trade.get("user")
        if isinstance(user_obj, dict):
            trader_address = str(user_obj.get("id", ""))
        else:
            trader_address = str(trade.get("proxyWallet") or trade.get("user", "") or "")
        
        # Extract side (buy/sell)
        side = str(trade.get("side", "")).lower() or "unknown"

        # Validate required fields
        if not trader_address or timestamp <= 0:
            return None

        return (timestamp, price, size, trader_address, market_id, side)
    except (ValueError, KeyError, TypeError):
        return None


@beartype
def fetch_trades(
    condition_id: str,
    limit: int = 500,
    api_client: PolymarketAPIClient | None = None,
) -> list[tuple[int, float, float, str, str, str]]:
    """
    Fetch and parse trades from Polymarket REST API.

    Args:
        condition_id: Market conditionId (not numeric ID)
        limit: Maximum number of trades to fetch
        api_client: Optional API client (creates new if None)

    Returns:
        List of parsed trades as tuples (timestamp, price, size, trader_address, market_id, side)
    """
    should_close = api_client is None
    if api_client is None:
        api_client = PolymarketAPIClient()

    try:
        # Fetch trades from REST API
        trades_data = api_client.get_trades(condition_id, limit=limit)

        # Parse all trades
        parsed_trades: list[tuple[int, float, float, str, str, str]] = []
        for trade in trades_data:
            parsed = parse_trade_data(trade, condition_id)
            if parsed:
                parsed_trades.append(parsed)

        return parsed_trades
    finally:
        if should_close:
            api_client.close()


@beartype
def fetch_trades_from_blockchain(
    condition_id: str,
    contract_address: str | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
) -> int:
    """
    Fetch all trades from blockchain using optimized parallel processing.

    Args:
        condition_id: Market conditionId to filter by
        contract_address: Optional contract address (uses config default if None)
        from_block: Optional starting block (uses contract deployment if None)
        to_block: Optional ending block (uses current block if None)

    Returns:
        Number of trades inserted into database
    """
    parser = OptimizedBlockchainParser(contract_address=contract_address)
    try:
        return parser.fetch_all_trades(
            condition_id=condition_id,
            from_block=from_block,
            to_block=to_block,
        )
    finally:
        parser.close()
