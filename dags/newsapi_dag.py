import os
import requests
import json
import logging
import pathlib
#from news_api_btc_raw import fetch_news_api
#from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv



# Get API Key from environment variable
#load_dotenv()
#NEWS_API_KEY = os.getenv("NEWS_API_KEY", "default_value")
NEWS_API_KEY  = os.getenv("NEWS_API_KEY")
#print('outside Key',NEWS_API_KEY )
GCS_BUCKET = "crypto-sentiment-analysis"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def fetch_dag_newsapi (**kwargs):
    today_date = datetime.today()
    yesterday = today_date - timedelta(days=1)
    yesterday_format = yesterday.strftime('%Y-%m-%d')
    keyword = "Bitcoin"
        # NewsAPI endpoint
    url = ('https://newsapi.org/v2/everything?'
       f'q={keyword}&'
       f'from={yesterday_format}&'
       f'apikey={NEWS_API_KEY}&'
       'sortBy=popularity&'
       'language=en'
        )
    #'to': date,    # End date (ISO format: YYYY-MM-DD)
     #f'apikey={api_key}&'
    # Send GET request to NewsAPI
    response = requests.get(url)
    if response.status_code == 200:
     print(url)
     #articles = response.json()['articles']
     articles = response.json()
     print(response.status_code)
     news_api_json = articles
     print(articles)
     log.info(f"Fetched Bitcoin New API Data for {yesterday_format }: {news_api_json}")
    #print(yesterday_format)
    else:
    #news_api_json = articles
     print(response.status_code)
     print(url)
     log.info(f"Failed to fetch articles. Status code: {response.status_code}")

    #Store News API to GCS
    # Store log in GCS (coingecko_bronze layer)
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    print(bucket)
    blob_name = f"coingecko_bronze/fetch_newsapi_{yesterday_format}.json"
    #blob_name = f"coingecko_bronze/fetch_newsapi_{yesterday_format}.txt"
    blob = bucket.blob(blob_name)

    try:
            #blob.upload_from_string(news_api_json, content_type="text/plain")
            blob.upload_from_string(news_api_json, content_type="application/json")
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
    "fetch_btc_newsapi_to_gcs",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=True
)

fetch_newsapi_task = PythonOperator(
    task_id="fetch_newsapi_task",
    python_callable=fetch_dag_newsapi,
    provide_context=True,
    dag=dag
)


#if __name__ == "__main__":
  # fetch_dag_newsapi()
