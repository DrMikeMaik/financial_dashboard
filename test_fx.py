"""Test script for NBP FX adapter."""
from app.adapters.fx_nbp import get_current_rates, get_rate_on_date, convert_to_pln
from datetime import date
from decimal import Decimal


def main():
    print("Testing NBP FX Adapter\n")
    print("=" * 50)

    # Test 1: Get current rates
    print("\n1. Fetching current NBP rates...")
    rates = get_current_rates()
    print(f"   Found {len(rates)} currencies")
    print("\n   Sample rates to PLN:")
    for ccy in ["USD", "EUR", "GBP", "CHF", "JPY"]:
        if ccy in rates:
            print(f"   {ccy}: {rates[ccy]}")

    # Test 2: Historical rate
    print("\n2. Fetching historical rate (USD on 2024-01-15)...")
    historical = get_rate_on_date("USD", "PLN", date(2024, 1, 15))
    if historical:
        print(f"   USD rate on 2024-01-15: {historical}")
    else:
        print("   Failed to fetch historical rate")

    # Test 3: Conversion
    print("\n3. Converting 100 USD to PLN...")
    if "USD" in rates:
        pln_amount = convert_to_pln(Decimal("100"), "USD", rates)
        print(f"   100 USD = {pln_amount} PLN")

    print("\n" + "=" * 50)
    print("✓ NBP adapter test complete\n")


if __name__ == "__main__":
    main()
