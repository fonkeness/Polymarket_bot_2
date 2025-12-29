"""Trade data parsing and transformation logic."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from beartype import beartype

if TYPE_CHECKING:
    from collections.abc import Sequence

from src.database.repository import insert_trades_batch
from src.parser.api_client import AsyncPolymarketAPIClient, PolymarketAPIClient


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
async def async_fetch_all_trades(
    condition_id: str,
    api_client: AsyncPolymarketAPIClient | None = None,
    limit_per_page: int = 1000,
    save_to_db: bool = True,
    progress_callback: Callable[[int, int], None] | None = None,
    max_trades: int = 1_000_000,
) -> int:
    """
    Optimized fetch ALL trades with duplicate prevention BEFORE parsing.

    This function uses offset-based pagination to fetch all historical trades.
    Results are sorted by timestamp descending (newest first).
    Trades are saved to database in batches to avoid memory issues.

    Key optimizations:
    - Checks uniqueness BEFORE parsing (saves CPU on duplicates)
    - Loads existing trades once at start
    - Uses single DB connection for all operations
    - Buffers trades before saving (every 2000 instead of every 500)

    Args:
        condition_id: Market conditionId (not numeric ID)
        api_client: Optional async API client (creates new if None)
        limit_per_page: Number of trades to fetch per page (max 1000 recommended)
        save_to_db: Whether to save trades to database
        progress_callback: Optional callback function(loaded_count, total_estimated) for progress
        max_trades: Maximum number of trades to fetch (protection against infinite loops)

    Returns:
        Total number of NEW trades fetched and saved
    """
    from src.database.connection import get_connection

    should_close = api_client is None
    if api_client is None:
        api_client = AsyncPolymarketAPIClient()

    # Load existing trades from DB ONCE at start
    existing_signatures = set()
    min_timestamp_in_db = None  # Запомнить минимальный timestamp из БД
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
            
            # Получить минимальный timestamp отдельным запросом
            cursor.execute(
                "SELECT MIN(timestamp) FROM trades WHERE market_id = ?",
                (condition_id,),
            )
            min_result = cursor.fetchone()
            min_timestamp_in_db = min_result[0] if min_result and min_result[0] else None
            
            print(f"✓ Loaded {len(existing_signatures):,} existing unique trades for filtering")
            if min_timestamp_in_db:
                from datetime import datetime
                print(f"✓ Oldest trade in DB: {datetime.fromtimestamp(min_timestamp_in_db).strftime('%Y-%m-%d %H:%M:%S')}")
        finally:
            db_conn_read.close()

    # Use single connection for all write operations
    db_conn = None
    if save_to_db:
        db_conn = get_connection()

    try:
        total_loaded = 0
        total_skipped = 0  # Counter for skipped duplicates
        offset = 0
        effective_limit = min(limit_per_page, 500)

        # Set of seen trades in this session (for fast checking)
        seen_signatures = existing_signatures.copy()

        # Infinite loop protection
        last_batch_signatures = None
        duplicate_batch_count = 0
        max_duplicate_batches = 5  # Увеличено с 3 до 5
        iterations = 0
        # Увеличить max_iterations - добавить больше запасных итераций
        max_iterations = (max_trades // effective_limit) * 3 + 2000  # Увеличено в 3 раза + большой запас

        # Buffer for batching DB operations
        batch_buffer: list[tuple[int, float, float, str, str, str]] = []
        BUFFER_SIZE = 2000  # Save every 2000 trades instead of every 500

        while True:
            iterations += 1

            # Protection 1: Maximum number of trades
            if total_loaded >= max_trades:
                print(f"\n⚠️  Reached maximum limit: {max_trades:,} trades. Stopping.")
                break

            # Protection 2: Maximum iterations
            if iterations > max_iterations:
                print(f"\n⚠️  Reached maximum iterations: {max_iterations}. Stopping.")
                break

            # Fetch trades batch
            trades_data = await api_client.get_trades(
                condition_id, limit=effective_limit, offset=offset
            )

            if not trades_data:
                print(f"\n✓ No more trades at offset {offset:,}. Finished.")
                break

            # OPTIMIZATION: Check uniqueness BEFORE parsing
            new_trades_data = []
            current_batch_signatures = set()

            for trade in trades_data:
                # Create signature WITHOUT parsing (fast!)
                signature = create_trade_signature(trade)

                if signature is None:
                    continue  # Skip invalid

                # Check for duplicates: not in DB and not in this session
                if signature not in seen_signatures:
                    new_trades_data.append(trade)
                    seen_signatures.add(signature)
                    current_batch_signatures.add(signature)
                else:
                    total_skipped += 1  # Count skipped duplicates

            # Protection 3: Check if we got the same data as last time
            # ИСПРАВЛЕНИЕ: Не считать пустые батчи как дубликаты
            # Пустой батч = все трейды были дубликатами, но это не значит что мы достигли конца
            if current_batch_signatures and last_batch_signatures and current_batch_signatures == last_batch_signatures:
                duplicate_batch_count += 1
                if duplicate_batch_count >= max_duplicate_batches:
                    # Проверить timestamp - может быть мы действительно достигли конца?
                    if trades_data:
                        timestamps = [t.get("timestamp", 0) for t in trades_data if t.get("timestamp")]
                        if timestamps:
                            oldest_timestamp = min(timestamps)
                            from datetime import datetime
                            oldest_date = datetime.fromtimestamp(oldest_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                            print(f"\n⚠️  Stopping: Got duplicate batches {max_duplicate_batches} times in a row.")
                            print(f"  Last trade timestamp: {oldest_date}")
                            
                            # Если timestamp очень старый (раньше чем в БД), возможно это действительно конец
                            if min_timestamp_in_db and oldest_timestamp >= min_timestamp_in_db:
                                print(f"  Note: Timestamp is newer than oldest in DB. May have reached end of available data.")
                            break
                    else:
                        print(f"\n⚠️  Stopping: Got duplicate batches {max_duplicate_batches} times in a row.")
                        break
            elif not current_batch_signatures and not last_batch_signatures:
                # Оба батча пустые (все дубликаты) - это нормально, продолжаем
                duplicate_batch_count = 0
            else:
                duplicate_batch_count = 0

            last_batch_signatures = current_batch_signatures

            # Parse ONLY unique trades (saves CPU!)
            parsed_trades: list[tuple[int, float, float, str, str, str]] = []
            for trade in new_trades_data:
                parsed = parse_trade_data(trade, condition_id)
                if parsed:
                    parsed_trades.append(parsed)

            # If no new trades in this batch
            if not parsed_trades:
                if len(trades_data) < effective_limit:
                    print(f"\n✓ No new trades and got fewer than requested. Finished.")
                    break
                # ИСПРАВЛЕНИЕ: Если получили полный батч, но все дубликаты - это нормально
                # Продолжаем загрузку, так как могут быть новые трейды дальше
                # НО: если offset очень большой и много дубликатов подряд, возможно достигли конца
                if offset > 500000 and duplicate_batch_count >= 5:
                    print(f"\n⚠️  Large offset ({offset:,}) with many duplicate batches. May have reached end.")
                    # Проверить timestamp
                    if trades_data:
                        timestamps = [t.get("timestamp", 0) for t in trades_data if t.get("timestamp")]
                        if timestamps:
                            oldest_timestamp = min(timestamps)
                            if min_timestamp_in_db and oldest_timestamp > min_timestamp_in_db + 86400:
                                print(f"  But timestamps suggest more data may exist. Continuing...")
                # If got full batch but all duplicates - continue
                offset += len(trades_data)
                continue

            # Add to buffer instead of immediate saving
            batch_buffer.extend(parsed_trades)

            # Save only when buffer is full
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
                estimated_total = (
                    total_loaded + effective_limit if len(trades_data) == effective_limit else total_loaded
                )
                progress_callback(total_loaded + len(batch_buffer), estimated_total)

            # Periodically show statistics
            if iterations % 50 == 0:
                print(
                    f"\n[Stats] Loaded: {total_loaded:,} | Skipped duplicates: {total_skipped:,} | Offset: {offset:,}"
                )

            # Check if we got fewer trades than requested (last page)
            if len(trades_data) < effective_limit:
                print(f"\n✓ Got fewer trades than requested ({len(trades_data)} < {effective_limit}). Finished.")
                break

            # Move to next page
            offset += len(trades_data)

            # Protection 4: Check that offset increased
            if offset % (effective_limit * 100) == 0 and offset > 0:
                print(f"\n[Debug] Offset: {offset:,}, Loaded: {total_loaded:,}, New in batch: {len(parsed_trades)}")
            
            # НОВОЕ: Проверить timestamp батча для отладки
            if trades_data and iterations % 20 == 0:  # Каждые 20 итераций
                timestamps = [t.get("timestamp", 0) for t in trades_data if t.get("timestamp")]
                if timestamps:
                    oldest_in_batch = min(timestamps)
                    newest_in_batch = max(timestamps)
                    from datetime import datetime
                    print(f"\n[Debug] Batch timestamps: {datetime.fromtimestamp(newest_in_batch).strftime('%Y-%m-%d %H:%M:%S')} to {datetime.fromtimestamp(oldest_in_batch).strftime('%Y-%m-%d %H:%M:%S')}")
                    if min_timestamp_in_db:
                        if oldest_in_batch < min_timestamp_in_db:
                            print(f"  ✓ Progress: Getting older trades (older than DB minimum)")

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

        print(f"\n✓ Finished after {iterations} iterations.")
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
                # Don't fail if volume calculation fails
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
