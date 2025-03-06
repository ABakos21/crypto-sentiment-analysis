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
#crypto-sentiment-analysis/crypto_silver_news
PROCESSED_CSV_PATH = "/tmp/processed_news.csv"

#crypto-sentiment-analysis/crypto_bronze_news

# Function to process JSON from GCS
from airflow.exceptions import AirflowSkipException
from google.cloud import storage
import json
from datetime import datetime

def extract_bitcoin_sentences(text):
    # Regular expression to split text into sentences
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', text)

    # Filter sentences containing the word 'bitcoin' (case-insensitive)
    bitcoin_sentences = [sentence for sentence in sentences if 'bitcoin' in sentence.lower()]

    return ' '.join(bitcoin_sentences)[:128]


def transform_newapi_data(ds, **kwargs):
    storage_client = storage.Client()
    bucket = storage_client.bucket("crypto-sentiment-analysis")

    # Convert ds (2025-03-04) to match your file format (04-03-2025)
    formatted_ds = datetime.strptime(ds, "%Y-%m-%d").strftime("%d-%m-%Y")
    #bronze_path = f"crypto_bronze_news/fetch_newsapi_{formatted_ds}.json"
    bronze_path = f"crypto_bronze_news/fetch_newsapi_2025-02-03.json"
    print('helloo  bronze_path', bronze_path)
    blob = bucket.blob(bronze_path)

    #crypto-sentiment-analysis/crypto_bronze_news/fetch_newsapi_2025-02-03.json

    # 🚨 Check if file exists, if not, skip task 🚨
    if not blob.exists():
        print(f"⚠️ No file found for {formatted_ds}. Skipping transformation.")
        raise AirflowSkipException(f"Skipping: No data available for {formatted_ds}")

    # ✅ If file exists, proceed
    raw_data = json.loads(blob.download_as_text())

    df = pd.DataFrame(raw_data["articles"])
    df["source_name"] = df["source"].apply(lambda x: x["name"] if x else None)
    df.drop(columns=["source"], inplace=True)
    df["publishedAt"] = pd.to_datetime(df["publishedAt"]).dt.strftime("%Y-%m-%d")
    #bitcoin_sentences = extract_bitcoin_sentences(text)
    df["description"] = df["description"].apply(extract_bitcoin_sentences) # Only store the first 100 characters
    df["content"] = df["content"].apply(extract_bitcoin_sentences)   # Limit the content length
    df = df[df["description"].apply(lambda x: len(x) > 0) & df["content"].apply(lambda x: len(x) > 0)]
    #df = df[["publishedAt", "source_name", "author", "title", "description", "content"]]
    df = df[["publishedAt", "source_name", "title", "description", "content"]]
    df.dropna(subset=["publishedAt"], inplace=True)
    # Replace any NaN values with empty strings to avoid NULL-related issues
    df.fillna("", inplace=True)

    date_prefix = df["publishedAt"].iloc[0]
    print('Helo date is',date_prefix)
    processed_csv_path = f"/tmp/newsapi_silver_{date_prefix}.csv"
    #processed_csv_path = f"/tmp/newsapi_silver_{date_prefix}.xlsx"
    #processed_csv = df.to_csv(processed_csv_path, index=False)
    # Save DataFrame to CSV with proper quoting
    df.to_csv(processed_csv_path, index=False,sep=',', encoding='utf-8',quotechar='"', quoting=csv.QUOTE_MINIMAL)
    #df.to_excel(processed_csv_path, index=False,engine='openpyxl')
    #df.to_csv(processed_csv_path, index=False, quoting=1, escapechar="\\")
    print('dataframe output',df.head())
    print('path file',processed_csv_path)
    # Push the filename & date to XCom
    kwargs['ti'].xcom_push(key='processed_csv_path', value=processed_csv_path)
    kwargs['ti'].xcom_push(key='date_prefix', value=date_prefix)
    return processed_csv_path


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
    "newapi_silver",
    default_args=default_args,
    description="Transform and load NewsAPI data from Bronze to Silver",
    schedule_interval="@daily",
    catchup=False,
)

# Task to transform data
transform_task = PythonOperator(
    task_id="transform_newapi_data",
    python_callable=transform_newapi_data,
    #op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    dag=dag,
)


#date_from_transform = "{{ ti.xcom_pull(task_ids='process_json_to_csv', key='date_prefix') }}"
#SILVER_PATH_TEMPLATE = f"crypto_silver_news/newsapi_silver_{date_from_transform}.csv"

        #dst="{{ ti.xcom_pull(task_ids='process_json_to_csv', key='date_prefix') }}_processed_news_data.csv",
upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="newapi_transformed_csv_to_gcs",
        #src=PROCESSED_CSV_PATH,
        #src=transform_task.output,
        src="{{ ti.xcom_pull(task_ids='transform_newapi_data', key='processed_csv_path') }}",
        #date_from_transform ="{{ ti.xcom_pull(task_ids='transform_newapi_data', key='date_prefix') }}",
        #SILVER_PATH_TEMPLATE = f"crypto_silver_news/newsapi_silver_{date_from_transform}.csv",
        #dst="crypto_silver_news/newsapi_silver_{{ ti.xcom_pull(task_ids='process_json_to_csv', key='date_prefix') }}.csv",
        dst="crypto_silver_news/newsapi_silver_{{ ti.xcom_pull(task_ids='transform_newapi_data', key='date_prefix') }}.csv",
        #dst="crypto_silver_news/newsapi_silver_{{ ti.xcom_pull(task_ids='transform_newapi_data', key='date_prefix') }}.xlsx",
        #dst=SILVER_PATH_TEMPLATE,
        bucket=GCS_BUCKET,
        mime_type="text/csv",
       # mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # MIME type for .xlsx)
       dag=dag,
)

#GCP_PROJECT_ID = "mimetic-parity-452009-b1"
#GCS_BUCKET = "crypto-sentiment-analysis"
#BRONZE_PATH_TEMPLATE = "crypto_bronze_news/fetch_newsapi_2025-02-03.json"
#SILVER_DATASET = "crypto_data_silver"
#SILVER_TABLE = "newsapi_silver"
#SILVER_PATH_TEMPLATE = "crypto_bronze_news/fetch_newsapi_{}.json"
#PROCESSED_CSV_PATH = "/tmp/processed_news.csv"



# Task to load transformed data into BigQuery
load_to_bigquery = GCSToBigQueryOperator(
    task_id="load_newsapi_to_bigquery",
    bucket=GCS_BUCKET,
    #source_objects=[SILVER_PATH_TEMPLATE.format("{{ ds_nodash }}")],
    source_objects=["crypto_silver_news/newsapi_silver_{{ ti.xcom_pull(task_ids='transform_newapi_data', key='date_prefix') }}.csv"],
    #source_objects=["crypto_silver_news/newsapi_silver_{{ ti.xcom_pull(task_ids='transform_newapi_data', key='date_prefix') }}.xlsx"],
    destination_project_dataset_table=f"{GCP_PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}",
    #source_format="NEWLINE_DELIMITED_JSON",
    source_format="CSV",
    #autodetect=True,
    schema_fields=[
                   {"name": "publishedAt", "type": "DATE", "mode": "REQUIRED"},
                   {"name": "source_name", "type": "STRING"},
                   {"name": "title", "type": "STRING"},
                   {"name": "description", "type": "STRING"},
                   {"name": "content", "type": "STRING"},
                              ],
    skip_leading_rows=1,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)


# Task dependencies
transform_task >>upload_to_gcs>>load_to_bigquery
