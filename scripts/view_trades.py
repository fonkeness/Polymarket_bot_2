"""Simple script to view trades from database."""

from pathlib import Path
import sys
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.repository import get_trades_by_market, get_trade_count


def view_trades(market_id: str, limit: int = 20) -> None:
    """View trades for a specific market."""
    print("=" * 100)
    print(f"Market ID: {market_id}")
    print("=" * 100)
    
    total = get_trade_count(market_id)
    print(f"Total trades: {total}\n")
    
    trades = get_trades_by_market(market_id, limit=limit)
    
    if not trades:
        print("No trades found.")
        return
    
    print(f"{'Date':<20} {'Price':<12} {'Size':<15} {'Trader Address':<45}")
    print("-" * 100)
    
    for trade in trades:
        date_str = datetime.fromtimestamp(trade['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        trader = trade['trader_address']
        if len(trader) > 43:
            trader = trader[:40] + '...'
        print(f"{date_str:<20} {trade['price']:<12.6f} {trade['size']:<15.6f} {trader:<45}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/view_trades.py <market_id> [limit]")
        print("Example: python scripts/view_trades.py 0x0576... 50")
        sys.exit(1)
    
    market_id = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    
    view_trades(market_id, limit)

