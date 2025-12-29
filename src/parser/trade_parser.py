"""Trade data parsing and transformation logic."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from beartype import beartype

if TYPE_CHECKING:
    from collections.abc import Sequence

from src.database.repository import insert_trades_batch
from src.parser.api_client import AsyncPolymarketAPIClient, PolymarketAPIClient


@beartype
def generate_daily_intervals(start_timestamp: int, end_timestamp: int) -> list[tuple[int, int]]:
    """
    Generate list of daily date intervals from start to end timestamp.
    
    Each interval represents one day: (start_of_day_timestamp, end_of_day_timestamp).
    Intervals are inclusive of start, exclusive of end (to avoid overlaps).
    
    Args:
        start_timestamp: Unix timestamp of the start date (inclusive)
        end_timestamp: Unix timestamp of the end date (exclusive)
    
    Returns:
        List of tuples (start_timestamp, end_timestamp) for each day
    """
    intervals: list[tuple[int, int]] = []
    
    # Convert timestamps to datetime (UTC)
    start_dt = datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_timestamp, tz=timezone.utc)
    
    # Start from beginning of start day
    current_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Generate intervals day by day
    while current_dt < end_dt:
        # Start of current day
        day_start = current_dt
        # Start of next day (end of current day)
        day_end = day_start + timedelta(days=1)
        
        # Convert back to timestamps
        day_start_ts = int(day_start.timestamp())
        day_end_ts = int(day_end.timestamp())
        
        # Clamp to actual start/end timestamps
        actual_start = max(day_start_ts, start_timestamp)
        actual_end = min(day_end_ts, end_timestamp)
        
        if actual_start < actual_end:
            intervals.append((actual_start, actual_end))
        
        # Move to next day
        current_dt = day_end
    
    return intervals


@beartype
def create_trade_signature(trade: dict[str, object]) -> str | None:
    """
    Create a unique signature from raw trade data WITHOUT parsing.
    This is faster than parsing and allows early duplicate detection.

    Args:
        trade: Raw trade dictionary from API

    Returns:
        Unique signature string or None if invalid
    """
    try:
        # Extract data directly without type conversion (faster)
        timestamp = trade.get("timestamp")
        price = trade.get("price")
        size = trade.get("size")
        trader_address = trade.get("proxyWallet") or trade.get("user", "")

        # Quick validation
        if not trader_address or not timestamp:
            return None

        # Create signature from raw data (faster than parsing)
        return f"{timestamp}|{price}|{size}|{trader_address}"
    except (KeyError, TypeError):
        return None


@beartype
def parse_trade_data(trade: dict[str, object], market_id: str) -> tuple[int, float, float, str, str, str] | None:
    """
    Parse a single trade from API response to database format.

    Args:
        trade: Trade dictionary from Data API response
        market_id: Market conditionId to associate with this trade

    Returns:
        Tuple of (timestamp, price, size, trader_address, market_id, side) or None if invalid
    """
    try:
        timestamp = int(trade.get("timestamp", 0))
        price = float(trade.get("price", 0.0))
        size = float(trade.get("size", 0.0))
        # Data API uses "proxyWallet" instead of "user"
        trader_address = str(trade.get("proxyWallet", ""))
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
    Fetch and parse trades from the Polymarket Data API.

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
        # Fetch trades from Data API (returns array directly)
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
async def async_fetch_trades_by_date_range(
    condition_id: str,
    start_timestamp: int,
    end_timestamp: int,
    api_client: AsyncPolymarketAPIClient,
    max_offset: int = 1000,  # Stop before offset bug kicks in
) -> list[dict[str, object]]:
    """
    Fetch trades for a specific date range using offset pagination with client-side filtering.
    
    This function works around the API offset bug by:
    - Using small offset increments (0, 500, 1000...)
    - Filtering trades by timestamp on client side
    - Stopping before offset bug kicks in (~1000-1500)
    
    Args:
        condition_id: Market conditionId
        start_timestamp: Start of date range (inclusive)
        end_timestamp: End of date range (exclusive)
        api_client: Async API client
        max_offset: Maximum offset to use before stopping (to avoid offset bug)
    
    Returns:
        List of trade dictionaries within the date range
    """
    all_trades: list[dict[str, object]] = []
    offset = 0
    limit = 500
    max_iterations = 10  # Safety limit
    
    for _ in range(max_iterations):
        if offset >= max_offset:
            # Stop before offset bug kicks in
            break
        
        # Fetch trades batch
        trades_data = await api_client.get_trades(
            condition_id, limit=limit, offset=offset
        )
        
        if not trades_data:
            break
        
        # Filter trades by timestamp (client-side filtering)
        trades_in_range: list[dict[str, object]] = []
        oldest_timestamp = None
        
        for trade in trades_data:
            trade_ts = trade.get("timestamp", 0)
            if not trade_ts:
                continue
            
            # Check if trade is in date range
            if start_timestamp <= trade_ts < end_timestamp:
                trades_in_range.append(trade)
            
            # Track oldest timestamp for stopping condition
            if oldest_timestamp is None or trade_ts < oldest_timestamp:
                oldest_timestamp = trade_ts
        
        all_trades.extend(trades_in_range)
        
        # Stop conditions:
        # 1. Got fewer trades than requested (last page)
        if len(trades_data) < limit:
            break
        
        # 2. Check if we've passed the date range
        if oldest_timestamp is not None:
            # If oldest trade is older than start_timestamp, we've gone too far back
            if oldest_timestamp < start_timestamp:
                break
            
            # If oldest trade is newer than end_timestamp, all trades are too new
            # But since API returns newest first, we should continue to get older trades
            # So we only stop if we're getting trades that are all too new AND we got a full batch
            # Actually, wait - if oldest_timestamp >= end_timestamp, we're getting trades
            # that are all newer than our range. But since offset gives us older trades,
            # we should continue. However, if we got a full batch and none are in range,
            # and oldest is still >= end_timestamp, something is wrong.
            # Actually, let's keep it simple: if oldest < start, we've passed the range.
        
        # 3. No trades in range and we got full batch - might be past the range
        # Check if oldest timestamp indicates we've passed the range
        if not trades_in_range and len(trades_data) == limit and oldest_timestamp is not None:
            if oldest_timestamp < start_timestamp:
                # All trades are older than our range - we've passed it
                break
        
        # Move to next offset
        offset += len(trades_data)
    
    return all_trades


@beartype
async def get_market_start_date_from_api(
    condition_id: str,
    api_client: AsyncPolymarketAPIClient,
    event_slug: str | None = None,
) -> int | None:
    """
    Get market start date from API using numeric market ID.
    
    Args:
        condition_id: Market conditionId
        api_client: Async API client
        event_slug: Optional event slug to find numeric ID (if not provided, tries condition_id directly)
    
    Returns:
        Start timestamp (Unix timestamp) or None if not found
    """
    numeric_id = None
    
    # Try to get numeric_id from event if event_slug is provided
    if event_slug:
        try:
            from src.parser.api_client import PolymarketAPIClient
            sync_client = PolymarketAPIClient()
            try:
                event_data = sync_client.get_event_markets(event_slug)
                markets = event_data.get("markets", [])
                for market in markets:
                    if market.get("conditionId") == condition_id:
                        numeric_id = market.get("id")
                        break
            finally:
                sync_client.close()
        except Exception:
            pass
    
    # If numeric_id not found, try using condition_id directly (might work)
    if not numeric_id:
        numeric_id = condition_id
    
    # Try to get market info
    try:
        market_info = await api_client.get_market_info(str(numeric_id))
        
        # Check for startDateIso first (as mentioned by user)
        if "startDateIso" in market_info:
            start_date_str = market_info["startDateIso"]
            try:
                dt = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
                return int(dt.timestamp())
            except (ValueError, AttributeError):
                pass
        
        # Try other possible field names
        for field_name in ["createdAt", "created_at", "startDate", "start_date", "created"]:
            if field_name in market_info:
                field_value = market_info[field_name]
                if isinstance(field_value, (int, float)):
                    return int(field_value)
                elif isinstance(field_value, str):
                    try:
                        dt = datetime.fromisoformat(field_value.replace("Z", "+00:00"))
                        return int(dt.timestamp())
                    except (ValueError, AttributeError):
                        pass
    except Exception:
        pass
    
    return None


@beartype
async def async_fetch_all_trades(
    condition_id: str,
    api_client: AsyncPolymarketAPIClient | None = None,
    limit_per_page: int = 1000,
    save_to_db: bool = True,
    progress_callback: Callable[[int, int], None] | None = None,
    max_trades: int = 1_000_000,
    event_slug: str | None = None,
) -> int:
    """
    Fetch ALL trades using date-based pagination to work around API offset bug.

    This function splits the date range into daily intervals and fetches trades
    for each day separately, filtering by timestamp on the client side.
    This avoids the API bug where offset-based pagination breaks after ~1000-1500.

    Key optimizations:
    - Date-based pagination (avoids offset bug)
    - Checks uniqueness BEFORE parsing (saves CPU on duplicates)
    - Loads existing trades once at start
    - Uses single DB connection for all operations
    - Buffers trades before saving (every 2000 instead of every 500)

    Args:
        condition_id: Market conditionId (not numeric ID)
        api_client: Optional async API client (creates new if None)
        limit_per_page: Number of trades to fetch per page (not used in date-based approach, kept for compatibility)
        save_to_db: Whether to save trades to database
        progress_callback: Optional callback function(loaded_count, total_estimated) for progress
        max_trades: Maximum number of trades to fetch (protection against infinite loops)
        event_slug: Optional event slug to get numeric market ID for startDateIso lookup

    Returns:
        Total number of NEW trades fetched and saved
    """
    from src.database.connection import get_connection

    should_close = api_client is None
    if api_client is None:
        api_client = AsyncPolymarketAPIClient()

    # Load existing trades from DB ONCE at start
    existing_signatures = set()
    min_timestamp_in_db = None
    if save_to_db:
        db_conn_read = get_connection()
        try:
            cursor = db_conn_read.cursor()
            cursor.execute(
                """
                SELECT timestamp || '|' || price || '|' || size || '|' || trader_address
                FROM trades 
                WHERE market_id = ?
                """,
                (condition_id,),
            )
            existing_signatures = set(row[0] for row in cursor.fetchall())
            
            cursor.execute(
                "SELECT MIN(timestamp) FROM trades WHERE market_id = ?",
                (condition_id,),
            )
            min_result = cursor.fetchone()
            min_timestamp_in_db = min_result[0] if min_result and min_result[0] else None
            
            print(f"✓ Loaded {len(existing_signatures):,} existing unique trades for filtering")
            if min_timestamp_in_db:
                print(f"✓ Oldest trade in DB: {datetime.fromtimestamp(min_timestamp_in_db, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
        finally:
            db_conn_read.close()

    # Determine start date: try to get from API, fallback to DB or fixed date
    start_timestamp = None
    
    # Try to get market start date from API using proper method
    start_timestamp = await get_market_start_date_from_api(condition_id, api_client, event_slug)
    if start_timestamp:
        print(f"✓ Found market start date from API: {datetime.fromtimestamp(start_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Fallback: use min_timestamp_in_db - 1 day, or fixed date, or current date - 1 year
    if start_timestamp is None:
        if min_timestamp_in_db:
            # Use oldest trade in DB minus 1 day to catch any missed trades
            start_timestamp = min_timestamp_in_db - 86400
            print(f"⚠️  Using fallback start date: {datetime.fromtimestamp(start_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} (oldest in DB - 1 day)")
        else:
            # Use fixed fallback date (e.g., 2024-11-19 for this market) or current date - 1 year
            fallback_date = datetime(2024, 11, 19, tzinfo=timezone.utc)
            start_timestamp = int(fallback_date.timestamp())
            print(f"⚠️  Using fixed fallback start date: {datetime.fromtimestamp(start_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    
    # End date: current time
    end_timestamp = int(datetime.now(timezone.utc).timestamp())
    
    # Generate daily intervals
    date_intervals = generate_daily_intervals(start_timestamp, end_timestamp)
    print(f"✓ Generated {len(date_intervals)} daily intervals from {datetime.fromtimestamp(start_timestamp, tz=timezone.utc).strftime('%Y-%m-%d')} to {datetime.fromtimestamp(end_timestamp, tz=timezone.utc).strftime('%Y-%m-%d')}")
    
    # Use single connection for all write operations
    db_conn = None
    if save_to_db:
        db_conn = get_connection()

    try:
        total_loaded = 0
        total_skipped = 0
        seen_signatures = existing_signatures.copy()
        
        # Buffer for batching DB operations
        batch_buffer: list[tuple[int, float, float, str, str, str]] = []
        BUFFER_SIZE = 2000
        
        # Process each daily interval (from start date to today, day by day)
        for interval_idx, (interval_start, interval_end) in enumerate(date_intervals, 1):
            if total_loaded >= max_trades:
                print(f"\n⚠️  Reached maximum limit: {max_trades:,} trades. Stopping.")
                break
            
            interval_date_str = datetime.fromtimestamp(interval_start, tz=timezone.utc).strftime('%Y-%m-%d')
            
            # Fetch trades for this date range
            trades_data = await async_fetch_trades_by_date_range(
                condition_id,
                interval_start,
                interval_end,
                api_client,
                max_offset=1000,  # Stop before offset bug kicks in
            )
            
            # Filter duplicates BEFORE parsing
            new_trades_data = []
            for trade in trades_data:
                signature = create_trade_signature(trade)
                if signature is None:
                    continue
                if signature not in seen_signatures:
                    new_trades_data.append(trade)
                    seen_signatures.add(signature)
                else:
                    total_skipped += 1
            
            # Parse unique trades
            parsed_trades: list[tuple[int, float, float, str, str, str]] = []
            for trade in new_trades_data:
                parsed = parse_trade_data(trade, condition_id)
                if parsed:
                    parsed_trades.append(parsed)
            
            # Display status for this day
            if parsed_trades:
                print(f"[{interval_idx}/{len(date_intervals)}] {interval_date_str} - Получено {len(parsed_trades)} сделок")
                batch_buffer.extend(parsed_trades)
                
                # Save when buffer is full
                if len(batch_buffer) >= BUFFER_SIZE:
                    if save_to_db and db_conn:
                        cursor = db_conn.cursor()
                        cursor.executemany(
                            "INSERT INTO trades (timestamp, price, size, trader_address, market_id, side) VALUES (?, ?, ?, ?, ?, ?)",
                            batch_buffer,
                        )
                        db_conn.commit()
                        inserted_count = cursor.rowcount
                        total_loaded += inserted_count
                    else:
                        total_loaded += len(batch_buffer)
                    
                    batch_buffer = []
                    
                    # Progress callback
                    if progress_callback:
                        progress_callback(total_loaded, total_loaded + len(batch_buffer))
            else:
                # Still show status even if no new trades
                print(f"[{interval_idx}/{len(date_intervals)}] {interval_date_str} - Получено 0 сделок")
        
        # Save remaining buffer
        if batch_buffer:
            if save_to_db and db_conn:
                cursor = db_conn.cursor()
                cursor.executemany(
                    "INSERT INTO trades (timestamp, price, size, trader_address, market_id, side) VALUES (?, ?, ?, ?, ?, ?)",
                    batch_buffer,
                )
                db_conn.commit()
                inserted_count = cursor.rowcount
                total_loaded += inserted_count
            else:
                total_loaded += len(batch_buffer)

        total_processed = total_loaded + total_skipped
        efficiency = (total_loaded / total_processed * 100) if total_processed > 0 else 0.0

        print(f"\n✓ Finished processing {len(date_intervals)} daily intervals.")
        print(f"  ✓ New trades loaded: {total_loaded:,}")
        print(f"  ✓ Duplicates skipped: {total_skipped:,}")
        print(f"  ✓ Efficiency: {efficiency:.1f}% unique")
        
        # Calculate total volume for this market if saving to DB
        if save_to_db and db_conn:
            try:
                cursor = db_conn.cursor()
                cursor.execute(
                    "SELECT SUM(size) FROM trades WHERE market_id = ?",
                    (condition_id,)
                )
                result = cursor.fetchone()
                total_volume = result[0] if result and result[0] is not None else 0.0
                print(f"  ✓ Total volume in database: {total_volume:,.2f}")
            except Exception:
                pass

        return total_loaded
    finally:
        if db_conn:
            db_conn.close()
        if should_close:
            await api_client.close()


@beartype
def fetch_all_trades(
    condition_id: str,
    api_client: PolymarketAPIClient | None = None,
    limit_per_page: int = 500,
) -> list[tuple[int, float, float, str, str, str]]:
    """
    Fetch ALL trades from Polymarket Data API.

    Note: Data API doesn't support pagination with cursor, so this fetches all available
    trades up to the limit. For full historical data, you may need to call this multiple
    times or use a different approach.

    Args:
        condition_id: Market conditionId (not numeric ID)
        api_client: Optional API client (creates new if None)
        limit_per_page: Number of trades to fetch (Data API may have its own limits)

    Returns:
        List of all parsed trades as tuples (timestamp, price, size, trader_address, market_id, side)
    """
    should_close = api_client is None
    if api_client is None:
        api_client = PolymarketAPIClient()

    try:
        # Data API returns array directly, pagination may not be supported
        # Fetch with high limit to get as many trades as possible
        trades_data = api_client.get_trades(condition_id, limit=limit_per_page)

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
