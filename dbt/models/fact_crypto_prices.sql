{{ config(
    materialized='table',
    schema='crypto_data_gold'
) }}

SELECT
    CONCAT(CAST(date_id AS STRING), '_', coin_id) AS fact_crypto_prices_id,  -- Primary Key
    date_id,
    coin_id,
    price_usd AS price,
    market_cap_usd AS market_cap,
    volume_usd_24h AS volume
FROM {{ source('crypto_data_silver', 'coingecko_silver') }}
