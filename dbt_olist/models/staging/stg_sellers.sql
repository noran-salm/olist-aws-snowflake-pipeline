-- models/staging/stg_sellers.sql

WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'dim_sellers') }}
)

SELECT
    seller_id,
    zip_code_prefix,
    INITCAP(city) AS city,
    UPPER(state)  AS state
FROM source
WHERE seller_id IS NOT NULL
