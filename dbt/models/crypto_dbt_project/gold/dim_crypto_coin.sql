{{ config(
    materialized='table',
    schema='crypto_data_gold'
) }}

WITH coin_metadata AS (
    SELECT DISTINCT
        coin AS coin_id,          -- Primary Key
        MIN(last_updated) OVER (PARTITION BY coin) AS first_historical_date
    FROM {{ source('crypto_data_silver', 'coingecko_silver') }}
)

SELECT
    coin_id,
    first_historical_date
FROM coin_metadata
