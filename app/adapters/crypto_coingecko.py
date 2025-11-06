"""CoinGecko crypto price adapter."""
import requests
import time
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Optional


COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"


def get_current_prices(symbols: List[str], vs_currency: str = "usd") -> Dict[str, Decimal]:
    """
    Fetch current prices for multiple cryptocurrencies.

    Args:
        symbols: List of crypto symbols (e.g., ["BTC", "ETH"])
        vs_currency: Currency to price in (e.g., "usd", "pln")

    Returns:
        Dict of {symbol: price}. Symbols not found will be omitted.
    """
    if not symbols:
        return {}

    # CoinGecko uses IDs, not symbols. Common mappings:
    symbol_to_id = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "USDT": "tether",
        "BNB": "binancecoin",
        "SOL": "solana",
        "USDC": "usd-coin",
        "XRP": "ripple",
        "ADA": "cardano",
        "DOGE": "dogecoin",
        "TRX": "tron",
        "DOT": "polkadot",
        "MATIC": "matic-network",
        "LTC": "litecoin",
        "SHIB": "shiba-inu",
        "AVAX": "avalanche-2",
    }

    # Convert symbols to IDs
    ids = []
    symbol_map = {}  # id -> symbol
    for symbol in symbols:
        symbol_upper = symbol.upper()
        coin_id = symbol_to_id.get(symbol_upper, symbol.lower())
        ids.append(coin_id)
        symbol_map[coin_id] = symbol_upper

    ids_str = ",".join(ids)

    try:
        url = f"{COINGECKO_BASE_URL}/simple/price"
        params = {
            "ids": ids_str,
            "vs_currencies": vs_currency.lower(),
        }
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        prices = {}
        for coin_id, price_data in data.items():
            if vs_currency.lower() in price_data:
                symbol = symbol_map.get(coin_id, coin_id.upper())
                prices[symbol] = Decimal(str(price_data[vs_currency.lower()]))

        return prices

    except Exception as e:
        print(f"Error fetching CoinGecko prices: {e}")
        return {}


def get_price(symbol: str, vs_currency: str = "usd") -> Optional[Decimal]:
    """
    Fetch current price for a single cryptocurrency.

    Args:
        symbol: Crypto symbol (e.g., "BTC")
        vs_currency: Currency to price in (e.g., "usd", "pln")

    Returns:
        Price as Decimal, or None if not found
    """
    prices = get_current_prices([symbol], vs_currency)
    return prices.get(symbol.upper())


def get_historical_prices(symbol: str, days: int = 30, vs_currency: str = "usd") -> List[tuple[datetime, Decimal]]:
    """
    Fetch historical daily prices for a cryptocurrency.

    Args:
        symbol: Crypto symbol (e.g., "BTC")
        days: Number of days of history (max 365 for free tier)
        vs_currency: Currency to price in

    Returns:
        List of (timestamp, price) tuples
    """
    # Map symbol to CoinGecko ID
    symbol_to_id = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "USDT": "tether",
        "BNB": "binancecoin",
        "SOL": "solana",
        "USDC": "usd-coin",
        "XRP": "ripple",
        "ADA": "cardano",
        "DOGE": "dogecoin",
        "TRX": "tron",
        "DOT": "polkadot",
        "MATIC": "matic-network",
        "LTC": "litecoin",
        "SHIB": "shiba-inu",
        "AVAX": "avalanche-2",
    }

    coin_id = symbol_to_id.get(symbol.upper(), symbol.lower())

    try:
        url = f"{COINGECKO_BASE_URL}/coins/{coin_id}/market_chart"
        params = {
            "vs_currency": vs_currency.lower(),
            "days": days,
            "interval": "daily",
        }
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        prices = []
        if "prices" in data:
            for timestamp_ms, price in data["prices"]:
                dt = datetime.fromtimestamp(timestamp_ms / 1000)
                prices.append((dt, Decimal(str(price))))

        return prices

    except Exception as e:
        print(f"Error fetching historical prices for {symbol}: {e}")
        return []


def search_coin(query: str) -> List[Dict]:
    """
    Search for a cryptocurrency by name or symbol.

    Args:
        query: Search term (e.g., "bitcoin", "BTC")

    Returns:
        List of matching coins with id, symbol, name
    """
    try:
        url = f"{COINGECKO_BASE_URL}/search"
        params = {"query": query}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        results = []
        if "coins" in data:
            for coin in data["coins"][:10]:  # Limit to top 10 results
                results.append({
                    "id": coin.get("id"),
                    "symbol": coin.get("symbol", "").upper(),
                    "name": coin.get("name"),
                })

        return results

    except Exception as e:
        print(f"Error searching for coin: {e}")
        return []
