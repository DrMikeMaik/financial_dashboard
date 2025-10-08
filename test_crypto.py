"""Test script for CoinGecko crypto adapter."""
from app.adapters.crypto_coingecko import get_current_prices, get_price, get_historical_prices, search_coin


def main():
    print("Testing CoinGecko Crypto Adapter\n")
    print("=" * 50)

    # Test 1: Get multiple prices in USD
    print("\n1. Fetching current crypto prices in USD...")
    prices_usd = get_current_prices(["BTC", "ETH", "SOL"], vs_currency="usd")
    for symbol, price in prices_usd.items():
        print(f"   {symbol}: ${price:,.2f}")

    # Test 2: Get single price in PLN
    print("\n2. Fetching BTC price in PLN...")
    btc_pln = get_price("BTC", vs_currency="pln")
    if btc_pln:
        print(f"   BTC: {btc_pln:,.2f} PLN")

    # Test 3: Historical prices (last 7 days)
    print("\n3. Fetching ETH historical prices (last 7 days)...")
    historical = get_historical_prices("ETH", days=7, vs_currency="usd")
    if historical:
        print(f"   Found {len(historical)} data points")
        if len(historical) > 0:
            latest = historical[-1]
            print(f"   Latest: {latest[0].strftime('%Y-%m-%d')} - ${latest[1]:,.2f}")

    # Test 4: Search for a coin
    print("\n4. Searching for 'cardano'...")
    results = search_coin("cardano")
    if results:
        for coin in results[:3]:
            print(f"   {coin['symbol']}: {coin['name']} (id: {coin['id']})")

    print("\n" + "=" * 50)
    print("✓ CoinGecko adapter test complete\n")


if __name__ == "__main__":
    main()
