WITH date_series AS (
    SELECT
        DATE('2025-01-01') + INTERVAL n DAY AS full_date
    FROM UNNEST(GENERATE_ARRAY(0, 364)) AS n
)

SELECT
    FORMAT_DATE('%Y%m%d', full_date) AS date_id,  -- Primary Key
    full_date,
    EXTRACT(YEAR FROM full_date) AS year,
    EXTRACT(MONTH FROM full_date) AS month,
    EXTRACT(DAY FROM full_date) AS day,
    EXTRACT(DAYOFWEEK FROM full_date) AS day_of_week,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM full_date) IN (1,7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    FORMAT_DATE('%B', full_date) AS month_name
FROM date_series
