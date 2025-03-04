from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
from google.cloud import storage
from airflow.exceptions import AirflowSkipException


# Define DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 3),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "coingecko_silver",
    default_args=default_args,
    description="Transform and load CoinGecko data from Bronze to Silver",
    schedule_interval="@daily",
    catchup=False,
)

# Google Cloud Storage & BigQuery settings
GCP_PROJECT_ID = "mimetic-parity-452009-b1"
GCS_BUCKET = "crypto-sentiment-analysis"
BRONZE_PATH_TEMPLATE = "crypto_bronze_prices/fetch_btc_01-02-2025.json"
SILVER_DATASET = "crypto_data_silver"
SILVER_TABLE = "coingecko_silver"
SILVER_PATH_TEMPLATE = "crypto_silver_prices/fetch_coingecko_{}.json"

# Function to process JSON from GCS
from airflow.exceptions import AirflowSkipException
from google.cloud import storage
import json
from datetime import datetime

def transform_coingecko_data(ds, **kwargs):
    storage_client = storage.Client()
    bucket = storage_client.bucket("crypto-sentiment-analysis")

    # Convert ds (2025-03-04) to match your file format (04-03-2025)
    formatted_ds = datetime.strptime(ds, "%Y-%m-%d").strftime("%d-%m-%Y")
    bronze_path = f"crypto_bronze_prices/fetch_btc_{formatted_ds}.json"
    blob = bucket.blob(bronze_path)

    # 🚨 Check if file exists, if not, skip task 🚨
    if not blob.exists():
        print(f"⚠️ No file found for {formatted_ds}. Skipping transformation.")
        raise AirflowSkipException(f"Skipping: No data available for {formatted_ds}")

    # ✅ If file exists, proceed
    raw_data = json.loads(blob.download_as_text())

    transformed_data = {
        "price_usd": round(float(raw_data.get("usd", 0.0)), 2) if raw_data.get("usd") is not None else None,
        "market_cap_usd": round(float(raw_data.get("usd_market_cap", 0.0)), 2) if raw_data.get("usd_market_cap") is not None else None,
        "volume_usd_24h": round(float(raw_data.get("usd_24h_vol", 0.0)), 2) if raw_data.get("usd_24h_vol") is not None else None,
        "coin": raw_data.get("coin", "unknown"),
    }

    # Handle date conversion for `last_updated`
    last_updated_str = raw_data.get("last_updated", None)
    transformed_data["last_updated"] = (
        datetime.strptime(last_updated_str, "%d-%m-%Y").strftime("%Y-%m-%d")
        if last_updated_str else None
    )

    # Save transformed data to GCS Silver layer
    silver_blob = bucket.blob(f"crypto_silver_prices/fetch_coingecko_{formatted_ds}.json")
    silver_blob.upload_from_string(json.dumps([transformed_data]))



# Task to transform data
transform_task = PythonOperator(
    task_id="transform_coingecko_data",
    python_callable=transform_coingecko_data,
    op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    dag=dag,
)

# Task to load transformed data into BigQuery
load_to_bigquery = GCSToBigQueryOperator(
    task_id="load_coingecko_to_bigquery",
    bucket=GCS_BUCKET,
    source_objects=[SILVER_PATH_TEMPLATE.format("{{ ds_nodash }}")],
    destination_project_dataset_table=f"{GCP_PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}",
    source_format="NEWLINE_DELIMITED_JSON",
    autodetect=True,
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_NEVER",
    dag=dag,
)

# Task dependencies
transform_task >> load_to_bigquery
