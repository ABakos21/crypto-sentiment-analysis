import streamlit as st
from google.cloud import bigquery
import pandas as pd

# Set page configuration as the first command in the script
st.set_page_config(page_title="Crypto News Dashboard", layout="wide")

st.title("Crypto Sentiment Analysis")
st.write("Welcome to the Crypto Sentiment Analysis Dashboard! This dashboard displays the latest crypto news articles.")

# Set up the BigQuery client
client = bigquery.Client()

# Define your BigQuery query
GCP_PROJECT_ID = "mimetic-parity-452009-b1"
SILVER_DATASET = "crypto_data_silver"
SILVER_TABLE = "newsapi_silver"

QUERY = f"""
    SELECT *
    FROM `{GCP_PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}`
    ORDER BY publishedAt DESC
    LIMIT 100
"""

def load_data():
    query_job = client.query(QUERY)
    return query_job.to_dataframe()

# Streamlit UI
st.write("Fetching the latest news articles from BigQuery...")

# Load data
try:
    data = load_data()

    if not data.empty:
        st.dataframe(data)
    else:
        st.warning("No data available.")
except Exception as e:
    st.error(f"Error loading data from BigQuery: {e}")

# Google Cloud Settings
PROJECT_ID = "mimetic-parity-452009-b1"
DATASET_ID = "crypto_data_silver"
TABLE_ID = "coingecko_silver"

# Initialize BigQuery Client
client = bigquery.Client()

def fetch_data():
    """Fetches latest data from BigQuery"""
    query = f"""
    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    ORDER BY last_updated DESC
    LIMIT 10
    """
    df = client.query(query).to_dataframe()
    return df

# Streamlit App
st.write("Fetching latest CoinGecko Silver data...")
data = fetch_data()

if not data.empty:
    st.dataframe(data)
else:
    st.write("No data available.")
