SELECT
    FORMAT_DATE('%Y%m%d', last_updated) AS date_id,  -- Foreign Key to dim_date
    coin AS coin_id,  -- Foreign Key to dim_crypto_coin
    volume_usd_24h,
    market_cap_usd,
    price_usd
FROM {{ source('crypto_data_silver', 'coingecko_silver') }}
