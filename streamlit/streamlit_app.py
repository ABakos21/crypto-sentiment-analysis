import os
import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
import json

#for deployment
gcp_credentials = json.loads(st.secrets["GOOGLE_CLOUD_CREDENTIALS"])
credentials = service_account.Credentials.from_service_account_info(gcp_credentials)

# Initialize BigQuery client with credentials
client = bigquery.Client(credentials=credentials, project=gcp_credentials["project_id"])

# Set Google Credentials
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/andrew/code/ABakos21/crypto-sentiment-analysis/docker/airflow-gcs-key.json"

# Initialize BigQuery Client
#client = bigquery.Client()

# Project & Dataset
PROJECT_ID = "mimetic-parity-452009-b1"
DATASET_ID = "dev_crypto_data_gold"

st.set_page_config(page_title="Crypto Sentiment Dashboard", layout="wide")
st.title("📊 Crypto Sentiment & Market Analysis Dashboard")

# Function to Fetch Bitcoin Market Data
def fetch_price_data():
    query = f"""
        SELECT date_id, price_usd, volume_usd_24h, market_cap_usd
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_crypto_prices`
        ORDER BY date_id ASC
    """
    return client.query(query).to_dataframe()

# Function to Fetch Sentiment Data
def fetch_sentiment_data(source_filter=None):
    source_condition = f"AND source_id = '{source_filter}'" if source_filter else ""
    query = f"""
        SELECT date_id, SUM(positive_news) AS positive_news,
                        SUM(negative_news) AS negative_news,
                        SUM(neutral_news) AS neutral_news,
                        SUM(net_sentiment_score) AS net_sentiment_score
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_crypto_sentiment`
        WHERE 1=1 {source_condition}
        GROUP BY date_id
        ORDER BY date_id ASC
    """
    return client.query(query).to_dataframe()

# Function to Fetch News Source Bias Data
def fetch_news_bias():
    query = f"""
        SELECT s.source_id,
               SUM(s.positive_news) AS positive_news,
               SUM(s.negative_news) AS negative_news,
               SUM(s.neutral_news) AS neutral_news,
               SUM(s.net_sentiment_score) AS bias_score,
               n.total_articles
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_crypto_sentiment` s
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_crypto_news_sources` n
        ON s.source_id = n.source_id
        GROUP BY s.source_id, n.total_articles
        ORDER BY bias_score DESC
    """
    return client.query(query).to_dataframe()

# Function to Fetch News Articles
def fetch_news_articles():
    query = f"""
        SELECT date_id, source_id, title, description, content, sentiment
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_crypto_news_articles`
        ORDER BY date_id DESC
    """
    return client.query(query).to_dataframe()

# Load Data
price_data = fetch_price_data()
news_bias_data = fetch_news_bias()
news_articles = fetch_news_articles()

# Convert date_id to datetime with flexible formats
price_data["date_id"] = pd.to_datetime(price_data["date_id"], format='%Y%m%d', errors='coerce')
news_articles["date_id"] = pd.to_datetime(news_articles["date_id"], format='%Y%m%d', errors='coerce')

# Sidebar Filters
st.sidebar.header("Filter Options")
source_options = ["All"] + news_bias_data["source_id"].unique().tolist()
selected_source = st.sidebar.selectbox("Select News Source", source_options)

# Weekly Filter
st.sidebar.subheader("Select Date Range")
start_date = st.sidebar.date_input("Start Date", price_data["date_id"].min())
end_date = st.sidebar.date_input("End Date", price_data["date_id"].max())

sentiment_data = fetch_sentiment_data(None if selected_source == "All" else selected_source)
sentiment_data["date_id"] = pd.to_datetime(sentiment_data["date_id"], format='%Y%m%d', errors='coerce')

# Filter Data by Selected Date Range
price_data = price_data[(price_data["date_id"] >= pd.to_datetime(start_date)) & (price_data["date_id"] <= pd.to_datetime(end_date))]
sentiment_data = sentiment_data[(sentiment_data["date_id"] >= pd.to_datetime(start_date)) & (sentiment_data["date_id"] <= pd.to_datetime(end_date))]
news_articles = news_articles[(news_articles["date_id"] >= pd.to_datetime(start_date)) & (news_articles["date_id"] <= pd.to_datetime(end_date))]
if selected_source != "All":
    news_articles = news_articles[news_articles["source_id"] == selected_source]

# Create Layout
col1, col2 = st.columns(2)

# Bitcoin Price Chart
with col1:
    st.subheader("📈 Bitcoin Price Over Time")
    fig = px.line(price_data, x="date_id", y="price_usd", title="Bitcoin Price (USD)", labels={"date_id": "Date", "price_usd": "Price (USD)"})
    st.plotly_chart(fig, use_container_width=True)

# Sentiment Trend Chart with News Source Filter (Bar Chart)
with col2:
    st.subheader(f"📰 Sentiment Over Time ({selected_source if selected_source != 'All' else 'All Sources'})")
    fig = px.bar(sentiment_data, x="date_id", y=["positive_news", "negative_news", "neutral_news"],
                  title="Crypto News Sentiment", labels={"date_id": "Date"}, barmode='group')
    st.plotly_chart(fig, use_container_width=True)

# Compare Sentiment & Bitcoin Price Trends (Normalized Scale)
st.subheader("📉 Comparing Bitcoin Price & Sentiment Over Time")
comparison_data = price_data.merge(sentiment_data, on="date_id", how="inner")
comparison_data["price_usd_scaled"] = comparison_data["price_usd"] / comparison_data["price_usd"].max()
comparison_data["net_sentiment_score_scaled"] = comparison_data["net_sentiment_score"] / abs(comparison_data["net_sentiment_score"].max())
fig = px.line(comparison_data, x="date_id", y=["price_usd_scaled", "net_sentiment_score_scaled"],
              title="Bitcoin Price vs. Sentiment (Normalized)", labels={"date_id": "Date"})
st.plotly_chart(fig, use_container_width=True)

# News Articles Table Filtered by Selected Source
st.subheader(f"📰 Crypto News Articles from {selected_source}")
st.dataframe(news_articles[["date_id", "source_id", "title", "description", "content", "sentiment"]])

st.success("Dashboard Updated with Latest Data! ✅")
