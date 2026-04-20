
  create or replace   view OLIST_DW.STAGING.stg_customers
  
  
  
  
  as (
    -- models/staging/stg_customers.sql

WITH source AS (
    SELECT * FROM OLIST_DW.RAW.dim_customers
)

SELECT
    customer_id,
    customer_unique_id,
    zip_code_prefix,
    INITCAP(city)  AS city,
    UPPER(state)   AS state
FROM source
WHERE customer_id IS NOT NULL
  );

