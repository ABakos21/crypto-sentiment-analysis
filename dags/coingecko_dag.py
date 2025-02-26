import os
import requests
import json
import logging
from datetime import datetime
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

# Get API Key from environment variable
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
GCS_BUCKET = "crypto-sentiment-analysis"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def fetch_btc_price():
    """Fetches Bitcoin price data and logs it to Cloud Storage"""
    log.info(f"🔹 Checking COINGECKO_API_KEY: {COINGECKO_API_KEY}")

    if not COINGECKO_API_KEY:
        log.error("Coingecko API key is missing! Set 'coingecko' in environment variables.")
        return

    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_last_updated_at": "true"
    }

    headers = {"x-cg-api-key": COINGECKO_API_KEY}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        btc_data = data.get("bitcoin", {})

        btc_data["last_updated"] = datetime.utcfromtimestamp(
            btc_data.get("last_updated_at", 0)
        ).strftime('%Y-%m-%d %H:%M:%S')
        btc_data["coin"] = "bitcoin"

        btc_json = json.dumps(btc_data, indent=4, ensure_ascii=False)
        log.info(f"Fetched BTC Data: {btc_json}")

        # Store log in GCS (coingecko_bronze layer)
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)
        blob_name = f"coingecko_bronze/fetch_btc_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}.json"
        blob = bucket.blob(blob_name)

        try:
            blob.upload_from_string(btc_json, content_type="application/json")
            log.info(f"Saved log to GCS: {blob.public_url}")
        except Exception as e:
            log.error(f"Failed to upload data to GCS: {e}")

    except requests.exceptions.RequestException as e:
        log.error(f"API request failed: {e}")

# Define DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 1),
    "retries": 1
}

dag = DAG(
    "fetch_btc_closing_price_to_gcs",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

fetch_btc_task = PythonOperator(
    task_id="fetch_btc_task",
    python_callable=fetch_btc_price,
    dag=dag
)
