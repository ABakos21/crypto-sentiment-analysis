WITH deduplicated AS (
    SELECT
        FORMAT_DATE('%Y%m%d', publishedAt) AS date_id,
        source_name AS source_id,
        sentiment,
        ROW_NUMBER() OVER (
            PARTITION BY FORMAT_DATE('%Y%m%d', publishedAt), source_name, sentiment
            ORDER BY publishedAt DESC
        ) AS row_num
    FROM {{ source('crypto_data_silver', 'newsapi_silver') }}
),

daily_sentiment AS (
    SELECT
        date_id,
        source_id,
        COUNT(*) AS news_count,
        SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_news,
        SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_news,
        SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) AS neutral_news
    FROM deduplicated
    WHERE row_num = 1
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
