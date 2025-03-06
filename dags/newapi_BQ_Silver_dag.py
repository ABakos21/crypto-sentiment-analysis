from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta
import json
from google.cloud import storage
from airflow.exceptions import AirflowSkipException
import pandas as pd
import re
import csv


# Google Cloud Storage & BigQuery settings
GCP_PROJECT_ID = "mimetic-parity-452009-b1"
GCS_BUCKET = "crypto-sentiment-analysis"
BRONZE_PATH_TEMPLATE = "crypto_bronze_news/fetch_newsapi_2025-02-03.json"
SILVER_DATASET = "crypto_data_silver"
SILVER_TABLE = "newsapi_silver"
SILVER_PATH_TEMPLATE = "crypto_silver_news/newsapi_silver_{}.csv"
PROCESSED_CSV_PATH = "/tmp/processed_news.csv"


def extract_bitcoin_sentences(text):
    """Extract sentences containing the word 'bitcoin'."""
    if not text:  # Handle None values
        return ""

    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', text)
    bitcoin_sentences = [sentence for sentence in sentences if 'bitcoin' in sentence.lower()]

    return ' '.join(bitcoin_sentences)[:128]


def transform_newapi_data(ds, **kwargs):
    """Transform NewsAPI JSON from GCS and save as CSV."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)

    formatted_ds = datetime.strptime(ds, "%Y-%m-%d").strftime("%d-%m-%Y")
    bronze_path = BRONZE_PATH_TEMPLATE
    blob = bucket.blob(bronze_path)

    if not blob.exists():
        raise AirflowSkipException(f"Skipping: No data available for {formatted_ds}")

    raw_data = json.loads(blob.download_as_text())
    df = pd.DataFrame(raw_data.get("articles", []))

    if df.empty:
        raise AirflowSkipException(f"Skipping: No articles found for {formatted_ds}")

    # Extract source name safely
    df["source_name"] = df["source"].apply(lambda x: x.get("name", "") if isinstance(x, dict) else "")
    df.drop(columns=["source"], inplace=True)

    # Format and clean data
    df["publishedAt"] = pd.to_datetime(df["publishedAt"], errors='coerce').dt.strftime("%Y-%m-%d")
    df.dropna(subset=["publishedAt"], inplace=True)  # Remove invalid date rows


    df["description"] = df["description"].astype(str).apply(extract_bitcoin_sentences).replace(r'[\r\n]+', ' ', regex=True)
    df["content"] = df["content"].astype(str).apply(extract_bitcoin_sentences).replace(r'[\r\n]+', ' ', regex=True)

    df = df[["publishedAt", "source_name", "title", "description", "content"]]
    df.fillna("", inplace=True)

    # Ensure required columns are present
    df = df.dropna(subset=["publishedAt", "source_name", "title", "description", "content"])

    # Ensure we don't reference an empty DataFrame
    if df.empty:
        raise AirflowSkipException(f"Skipping: No valid data to process for {formatted_ds}")

    date_prefix = df["publishedAt"].iloc[0]
    processed_csv_path = f"/tmp/newsapi_silver_{date_prefix}.csv"

    required_columns = ["publishedAt", "source_name", "title", "description", "content"]
    df = df.dropna(subset=required_columns)


    df.to_csv(processed_csv_path, index=False, sep=',', encoding='utf-8',
          quotechar='"', quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n')

    kwargs['ti'].xcom_push(key='processed_csv_path', value=processed_csv_path)
    kwargs['ti'].xcom_push(key='date_prefix', value=date_prefix)
    return processed_csv_path


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 3),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "newapi_silver",
    default_args=default_args,
    description="Transform and load NewsAPI data from Bronze to Silver",
    schedule_interval="@daily",
    catchup=False,
)

transform_task = PythonOperator(
    task_id="transform_newapi_data",
    python_callable=transform_newapi_data,
    dag=dag,
)

upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id="newapi_transformed_csv_to_gcs",
    src="{{ ti.xcom_pull(task_ids='transform_newapi_data', key='processed_csv_path') }}",
    dst="crypto_silver_news/newsapi_silver_{{ ti.xcom_pull(task_ids='transform_newapi_data', key='date_prefix') }}.csv",
    bucket=GCS_BUCKET,
    mime_type="text/csv",
    dag=dag,
)

load_to_bigquery = GCSToBigQueryOperator(
    task_id="load_newsapi_to_bigquery",
    bucket=GCS_BUCKET,
    source_objects=["crypto_silver_news/newsapi_silver_{{ ti.xcom_pull(task_ids='transform_newapi_data', key='date_prefix') }}.csv"],
    destination_project_dataset_table=f"{GCP_PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}",
    source_format="CSV",
    schema_fields=[
        {"name": "publishedAt", "type": "DATE", "mode": "REQUIRED"},
        {"name": "source_name", "type": "STRING"},
        {"name": "title", "type": "STRING"},
        {"name": "description", "type": "STRING"},
        {"name": "content", "type": "STRING"},
    ],
    skip_leading_rows=1,
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

transform_task >> upload_to_gcs >> load_to_bigquery
