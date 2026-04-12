USE ROLE SYSADMIN;
USE DATABASE OLIST_DW;
USE SCHEMA RAW;
USE WAREHOUSE OLIST_WH;

-- ── Create all 4 Snowpipes ────────────────────────────────────

CREATE OR REPLACE PIPE OLIST_DW.RAW.pipe_dim_customers
    AUTO_INGEST = FALSE
AS
COPY INTO OLIST_DW.RAW.dim_customers
FROM (
    SELECT
        $1:customer_id::VARCHAR,
        $1:customer_unique_id::VARCHAR,
        $1:zip_code_prefix::VARCHAR,
        $1:city::VARCHAR,
        $1:state::VARCHAR,
        $1:dbt_updated_at::TIMESTAMP_NTZ
    FROM @OLIST_DW.RAW.olist_processed_stage/dim_customers/
)
FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE);

CREATE OR REPLACE PIPE OLIST_DW.RAW.pipe_dim_products
    AUTO_INGEST = FALSE
AS
COPY INTO OLIST_DW.RAW.dim_products
FROM (
    SELECT
        $1:product_id::VARCHAR,
        $1:category::VARCHAR,
        $1:name_length::INTEGER,
        $1:description_length::INTEGER,
        $1:photos_qty::INTEGER,
        $1:product_weight_kg::FLOAT,
        $1:length_cm::FLOAT,
        $1:height_cm::FLOAT,
        $1:width_cm::FLOAT,
        $1:dbt_updated_at::TIMESTAMP_NTZ
    FROM @OLIST_DW.RAW.olist_processed_stage/dim_products/
)
FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE);

CREATE OR REPLACE PIPE OLIST_DW.RAW.pipe_dim_sellers
    AUTO_INGEST = FALSE
AS
COPY INTO OLIST_DW.RAW.dim_sellers
FROM (
    SELECT
        $1:seller_id::VARCHAR,
        $1:zip_code_prefix::VARCHAR,
        $1:city::VARCHAR,
        $1:state::VARCHAR,
        $1:dbt_updated_at::TIMESTAMP_NTZ
    FROM @OLIST_DW.RAW.olist_processed_stage/dim_sellers/
)
FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE);

CREATE OR REPLACE PIPE OLIST_DW.RAW.pipe_fct_orders
    AUTO_INGEST = FALSE
AS
COPY INTO OLIST_DW.RAW.fct_orders
FROM (
    SELECT
        $1:order_id::VARCHAR,
        $1:order_item_id::INTEGER,
        $1:customer_id::VARCHAR,
        $1:seller_id::VARCHAR,
        $1:product_id::VARCHAR,
        $1:order_status::VARCHAR,
        $1:order_purchase_timestamp::TIMESTAMP_NTZ,
        $1:order_approved_at::TIMESTAMP_NTZ,
        $1:order_delivered_carrier_date::TIMESTAMP_NTZ,
        $1:order_delivered_customer_date::TIMESTAMP_NTZ,
        $1:order_estimated_delivery_date::TIMESTAMP_NTZ,
        $1:shipping_limit_date::TIMESTAMP_NTZ,
        $1:price::FLOAT,
        $1:freight_value::FLOAT,
        $1:order_item_revenue::FLOAT,
        $1:total_payment_value::FLOAT,
        $1:max_installments::INTEGER,
        PARSE_JSON($1:payment_types::VARCHAR),
        $1:avg_review_score::FLOAT,
        $1:delivery_days::INTEGER,
        $1:is_late_delivery::INTEGER,
        $1:order_year_month::VARCHAR
    FROM @OLIST_DW.RAW.olist_processed_stage/fct_orders/
)
FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE);

-- ── Verify pipes created ──────────────────────────────────────
SHOW PIPES IN SCHEMA OLIST_DW.RAW;