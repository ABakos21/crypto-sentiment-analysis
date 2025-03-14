SELECT DISTINCT
    source_name AS source_id,  -- Primary Key
    COUNT(*) OVER (PARTITION BY source_name) AS total_articles
FROM {{ source('crypto_data_silver', 'newsapi_silver') }}
