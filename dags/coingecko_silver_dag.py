from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import json
from google.cloud import storage
from airflow.exceptions import AirflowSkipException

# DAG Arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG Definition
dag = DAG(
    "coingecko_silver",
    default_args=default_args,
    description="Transform and load CoinGecko data from Bronze to Silver",
    schedule_interval="@daily",
    catchup=True,
)

# Google Cloud Storage & BigQuery settings
GCP_PROJECT_ID = "mimetic-parity-452009-b1"
GCS_BUCKET = "crypto-sentiment-analysis"
SILVER_DATASET = "crypto_data_silver"
SILVER_TABLE = "coingecko_silver"

# ✅ Wait for Bronze DAG before running
wait_for_bronze = ExternalTaskSensor(
    task_id="wait_for_bronze",
    external_dag_id="fetch_btc_closing_price_to_gcs",
    external_task_id="fetch_btc_task",
    execution_delta=timedelta(minutes=1),
    mode="poke",
    timeout=600,
    dag=dag,
)

def transform_coingecko_data(ds, **kwargs):
    """Transforms the latest CoinGecko JSON file for the day."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)

    # ✅ Convert Airflow execution date (YYYY-MM-DD) → DD-MM-YYYY
    formatted_ds = datetime.strptime(ds, "%Y-%m-%d").strftime("%d-%m-%Y")

    # ✅ Construct expected file path
    bronze_path = f"coingecko_bronze/fetch_btc_{formatted_ds}.json"
    blob = bucket.blob(bronze_path)

    # 🚨 Check if file exists, else skip task 🚨
    if not blob.exists():
        print(f"⚠️ No file found for {bronze_path}. Skipping transformation.")
        raise AirflowSkipException(f"Skipping: No data available for {formatted_ds}")

    # ✅ Read JSON file from GCS
    raw_data = json.loads(blob.download_as_text())

    # ✅ Transform Data
    transformed_data = {
        "price_usd": round(float(raw_data.get("usd", 0.0)), 2),
        "market_cap_usd": round(float(raw_data.get("usd_market_cap", 0.0)), 2),
        "volume_usd_24h": round(float(raw_data.get("usd_24h_vol", 0.0)), 2),
        "coin": raw_data.get("coin", "unknown"),
        "last_updated": datetime.strptime(raw_data.get("last_updated", formatted_ds), "%d-%m-%Y").strftime("%Y-%m-%d"),
    }

    # ✅ Save transformed JSON in Silver Layer (NDJSON format)
    silver_blob_name = f"crypto_silver_prices/fetch_coingecko_{ds}.json"
    silver_blob = bucket.blob(silver_blob_name)

    # ✅ Store in NDJSON format
    silver_blob.upload_from_string(json.dumps(transformed_data) + "\n", content_type="application/json")

    print(f"✅ Transformed and saved: {bronze_path} → {silver_blob_name}")

# ✅ Transform Task
transform_task = PythonOperator(
    task_id="transform_coingecko_data",
    python_callable=transform_coingecko_data,
    op_kwargs={"ds": "{{ ds }}"},
    provide_context=True,
    dag=dag,
)

# ✅ Load today's transformed data into BigQuery
load_to_bigquery = GCSToBigQueryOperator(
    task_id="load_coingecko_to_bigquery",
    bucket=GCS_BUCKET,
    source_objects=["crypto_silver_prices/fetch_coingecko_{{ ds }}.json"],
    destination_project_dataset_table=f"{GCP_PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}",
    source_format="NEWLINE_DELIMITED_JSON",
    autodetect=True,
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_IF_NEEDED",
    dag=dag,
)

# ✅ DAG Dependencies
wait_for_bronze >> transform_task >> load_to_bigquery
