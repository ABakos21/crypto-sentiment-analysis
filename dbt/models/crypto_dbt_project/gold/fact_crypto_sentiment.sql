WITH daily_sentiment AS (
    SELECT
        FORMAT_DATE('%Y%m%d', publishedAt) AS date_id,  -- Foreign Key to dim_date
        source_name AS source_id,  -- Foreign Key to dim_crypto_news_sources
        COUNT(*) AS news_count,
        SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_news,
        SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_news,
        SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) AS neutral_news
    FROM {{ source('crypto_data_silver', 'newsapi_silver') }}
    GROUP BY date_id, source_id
)

SELECT
    date_id,
    source_id,
    news_count,
    positive_news,
    negative_news,
    neutral_news,
    (positive_news - negative_news) AS net_sentiment_score
FROM daily_sentiment
