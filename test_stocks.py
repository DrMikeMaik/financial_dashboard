"""Test script for yfinance stocks adapter."""
from app.adapters.stocks_yfinance import (
    get_current_price,
    get_current_prices,
    get_info,
    get_historical_prices,
    get_dividends,
    get_splits
)
from datetime import date, timedelta


def main():
    print("Testing yfinance Stocks Adapter\n")
    print("=" * 50)

    # Test 1: Get single stock price
    print("\n1. Fetching AAPL current price...")
    aapl_price = get_current_price("AAPL")
    if aapl_price:
        print(f"   AAPL: ${aapl_price}")

    # Test 2: Get multiple stock prices
    print("\n2. Fetching multiple stock prices...")
    prices = get_current_prices(["MSFT", "GOOGL", "SPY"])
    for symbol, price in prices.items():
        print(f"   {symbol}: ${price}")

    # Test 3: Get stock info
    print("\n3. Fetching AAPL info...")
    info = get_info("AAPL")
    print(f"   Name: {info.get('name')}")
    print(f"   Currency: {info.get('currency')}")
    print(f"   Exchange: {info.get('exchange')}")

    # Test 4: Historical prices (last 7 days)
    print("\n4. Fetching AAPL historical prices (last 7 days)...")
    start = date.today() - timedelta(days=7)
    historical = get_historical_prices("AAPL", start_date=start)
    if historical:
        print(f"   Found {len(historical)} data points")
        if len(historical) > 0:
            latest = historical[-1]
            print(f"   Latest: {latest[0].strftime('%Y-%m-%d')} - ${latest[1]}")

    # Test 5: Recent dividends
    print("\n5. Fetching AAPL dividends (last year)...")
    dividends = get_dividends("AAPL")
    if dividends:
        print(f"   Found {len(dividends)} dividend payments")
        if len(dividends) > 0:
            latest_div = dividends[-1]
            print(f"   Latest: {latest_div[0].strftime('%Y-%m-%d')} - ${latest_div[1]}")
    else:
        print("   No dividends found")

    print("\n" + "=" * 50)
    print("✓ yfinance adapter test complete\n")


if __name__ == "__main__":
    main()
