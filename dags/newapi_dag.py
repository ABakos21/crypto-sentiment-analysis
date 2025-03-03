import os
import requests
import json
import logging
from datetime import datetime, timedelta
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Get API Key from environment variable
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
GCS_BUCKET = "crypto-sentiment-analysis"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def fetch_dag_newsapi(**kwargs):
    execution_date = (datetime.strptime(kwargs["ds"], "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    log.info(f"Fetching news data for {execution_date}")

    keyword = "Bitcoin"
    url = (
        f"https://newsapi.org/v2/everything?"
        f"q={keyword}&"
        f"from={execution_date}&"
        f"to={execution_date}&"
        f"sortBy=popularity&"
        f"language=en"
    )

    # Set custom headers to avoid HTTP 426 error
    headers = {
        "User-Agent": "Mozilla/5.0",
        "X-Api-Key": NEWS_API_KEY
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        articles = response.json().get("articles", [])

        # Skip upload if no news found
        if not articles:
            log.warning(f"⚠️ No news articles found for {execution_date}. Skipping GCS upload.")
            return

        log.info(f"✅ Successfully fetched {len(articles)} articles for {execution_date}")

    except requests.exceptions.RequestException as e:
        log.error(f"❌ API request failed: {e}")
        return

    # Store News API response to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob_name = f"crypto_bronze_news/fetch_newsapi_{execution_date}.json"
    blob = bucket.blob(blob_name)

    try:
        news_api_json_str = json.dumps(articles, indent=4)
        blob.upload_from_string(news_api_json_str, content_type="application/json")
        log.info(f"✅ Successfully saved news data to GCS: {blob_name}")

    except Exception as e:
        log.error(f"❌ Failed to upload data to GCS: {e}")

# Define DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(30),
    "retries": 1,
}

dag = DAG(
    "fetch_btc_newsapi_to_gcs",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

fetch_newsapi_task = PythonOperator(
    task_id="fetch_newsapi_task",
    python_callable=fetch_dag_newsapi,
    provide_context=True,
    dag=dag
)
