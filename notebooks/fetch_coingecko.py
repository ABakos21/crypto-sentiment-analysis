import os
import requests
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
COINGECKO_API_KEY = os.getenv("coingecko")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_crypto_prices(coin_list=None):
    """
    Fetches cryptocurrency price and market data from CoinGecko API for multiple coins.

    Parameters:
    - coin_list (list): A list of coin IDs to fetch (e.g., ["bitcoin", "ethereum", "ripple"])

    Returns:
    - List of dictionaries containing the price, market cap, volume, and last updated timestamp.
    """
    if coin_list is None:
        coin_list = ["bitcoin"]

    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": ",".join(coin_list),
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_last_updated_at": "true"
    }

    headers = {
        "x-cg-api-key": COINGECKO_API_KEY
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()

        data = response.json()
        results = []

        for coin in coin_list:
            coin_data = data.get(coin, {})

            # Ensure all expected fields exist
            price = coin_data.get("usd", "N/A")
            market_cap = coin_data.get("usd_market_cap", "N/A")
            volume_24h = coin_data.get("usd_24h_vol", "N/A")
            last_updated_ts = coin_data.get("last_updated_at", 0)

            # Convert timestamp
            last_updated = datetime.utcfromtimestamp(last_updated_ts).strftime('%Y-%m-%d %H:%M:%S') if last_updated_ts else "N/A"

            # Structure final output
            result = {
                "coin_name": coin,
                "price_usd": price,
                "market_cap": market_cap,
                "24h_volume": volume_24h,
                "last_updated": last_updated
            }

            results.append(result)

        logging.info(f"Fetched Crypto Data: {json.dumps(results, indent=4)}")
        return results

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None

if __name__ == "__main__":
    fetch_crypto_prices(["bitcoin", "ethereum", "ripple"])
