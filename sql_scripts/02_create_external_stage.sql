USE ROLE SYSADMIN;

-- ── Database & Schemas ────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS OLIST_DW;

CREATE SCHEMA IF NOT EXISTS OLIST_DW.RAW;
CREATE SCHEMA IF NOT EXISTS OLIST_DW.STAGING;
CREATE SCHEMA IF NOT EXISTS OLIST_DW.MARTS;

-- ── Warehouse ─────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS OLIST_WH
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'Olist pipeline warehouse';

USE ROLE SYSADMIN;
USE WAREHOUSE OLIST_WH;
USE DATABASE OLIST_DW;
USE SCHEMA RAW;

-- ── Step 1: Create Tables ─────────────────────────────────────

CREATE OR REPLACE TABLE OLIST_DW.RAW.dim_customers (
    customer_id         VARCHAR(50),
    customer_unique_id  VARCHAR(50),
    zip_code_prefix     VARCHAR(10),
    city                VARCHAR(100),
    state               VARCHAR(5),
    dbt_updated_at      TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE OLIST_DW.RAW.dim_products (
    product_id          VARCHAR(50),
    category            VARCHAR(100),
    name_length         INTEGER,
    description_length  INTEGER,
    photos_qty          INTEGER,
    product_weight_kg   FLOAT,
    length_cm           FLOAT,
    height_cm           FLOAT,
    width_cm            FLOAT,
    dbt_updated_at      TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE OLIST_DW.RAW.dim_sellers (
    seller_id       VARCHAR(50),
    zip_code_prefix VARCHAR(10),
    city            VARCHAR(100),
    state           VARCHAR(5),
    dbt_updated_at  TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE OLIST_DW.RAW.fct_orders (
    order_id                       VARCHAR(50),
    order_item_id                  INTEGER,
    customer_id                    VARCHAR(50),
    seller_id                      VARCHAR(50),
    product_id                     VARCHAR(50),
    order_status                   VARCHAR(30),
    order_purchase_timestamp       TIMESTAMP_NTZ,
    order_approved_at              TIMESTAMP_NTZ,
    order_delivered_carrier_date   TIMESTAMP_NTZ,
    order_delivered_customer_date  TIMESTAMP_NTZ,
    order_estimated_delivery_date  TIMESTAMP_NTZ,
    shipping_limit_date            TIMESTAMP_NTZ,
    price                          FLOAT,
    freight_value                  FLOAT,
    order_item_revenue             FLOAT,
    total_payment_value            FLOAT,
    max_installments               INTEGER,
    payment_types                  VARIANT,
    avg_review_score               FLOAT,
    delivery_days                  INTEGER,
    is_late_delivery               INTEGER,
    order_year_month               VARCHAR(7)
)
CLUSTER BY (order_year_month);

-- Confirm all 4 tables exist
SHOW TABLES IN SCHEMA OLIST_DW.RAW;

-- ── Step 2: Load Data ─────────────────────────────────────────

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

-- ── Step 3: Verify Row Counts ─────────────────────────────────

SELECT 'dim_customers' AS table_name, COUNT(*) AS raws FROM OLIST_DW.RAW.dim_customers
UNION ALL SELECT 'dim_products',      COUNT(*) FROM OLIST_DW.RAW.dim_products
UNION ALL SELECT 'dim_sellers',       COUNT(*) FROM OLIST_DW.RAW.dim_sellers
UNION ALL SELECT 'fct_orders',        COUNT(*) FROM OLIST_DW.RAW.fct_orders
ORDER BY table_name;
