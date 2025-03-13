WITH deduplicated AS (
    SELECT
        date_id,
        source_id,
        title,
        description,
        content,
        sentiment,
        ROW_NUMBER() OVER (
            PARTITION BY date_id, source_id, title, content
            ORDER BY date_id DESC
        ) AS row_num
    FROM `mimetic-parity-452009-b1.dev_crypto_data_gold.fact_crypto_news_articles`
)

SELECT date_id, source_id, title, description, content, sentiment
FROM deduplicated
WHERE row_num = 1
