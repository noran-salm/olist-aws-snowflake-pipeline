cat > /mnt/d/Data-Engineering/Data-zoomcamp/Final-project/final_project_copy/olist-aws-snowflake-pipeline/sql_scripts/02_create_external_stage.sql << 'EOF'
-- ============================================================
-- 02_create_external_stage.sql
-- Run as SYSADMIN after 01_create_storage_integration.sql
-- Creates: warehouse, schemas, stage, tables, loads data
-- ============================================================

USE ROLE SYSADMIN;

-- ── Database & Schemas ────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS OLIST_DW;
CREATE SCHEMA  IF NOT EXISTS OLIST_DW.RAW;
CREATE SCHEMA  IF NOT EXISTS OLIST_DW.STAGING;
CREATE SCHEMA  IF NOT EXISTS OLIST_DW.MARTS;

-- ── Warehouse ─────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS OLIST_WH
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'Olist pipeline warehouse';

USE WAREHOUSE OLIST_WH;
USE DATABASE  OLIST_DW;
USE SCHEMA    RAW;

-- ── Stage (Direct Credentials — IAM user: olist-snowflake-s3-user) ───────────
-- NOTE: Cross-account role assumption was replaced with direct IAM user keys
-- because Snowflake's STS ExternalId handshake requires SQS setup.
CREATE OR REPLACE STAGE OLIST_DW.RAW.olist_processed_stage
    URL         = 's3://olist-lake-516671521715/processed/'
    CREDENTIALS = (
        AWS_KEY_ID     = '<YOUR_ACCESS_KEY_ID>'
        AWS_SECRET_KEY = '<YOUR_SECRET_KEY>'
    )
    FILE_FORMAT = (
        TYPE               = PARQUET
        SNAPPY_COMPRESSION = TRUE
        NULL_IF            = ('', 'null', 'NULL', 'None')
    )
    COMMENT = 'Points to Glue-processed Parquet files in S3';

-- Verify stage can list files
LIST @OLIST_DW.RAW.olist_processed_stage;

-- ── Tables ────────────────────────────────────────────────────
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
    order_year_month               VARCHAR(7)    -- derived from timestamp, not in Parquet
)
CLUSTER BY (order_year_month);

SHOW TABLES IN SCHEMA OLIST_DW.RAW;

-- ── COPY INTO ─────────────────────────────────────────────────

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
FILE_FORMAT = (TYPE=PARQUET SNAPPY_COMPRESSION=TRUE);

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
FILE_FORMAT = (TYPE=PARQUET SNAPPY_COMPRESSION=TRUE);

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
FILE_FORMAT = (TYPE=PARQUET SNAPPY_COMPRESSION=TRUE);

-- !! KEY FIX: order_year_month is a Spark partition column →
--    NOT stored inside the Parquet file.
--    Derive it from order_purchase_timestamp during load.
--    payment_types is an array → use TO_VARIANT, not PARSE_JSON.
COPY INTO OLIST_DW.RAW.fct_orders
FROM (
    SELECT
        $1:order_id::VARCHAR,
        $1:order_item_id::VARCHAR,        -- loaded as VARCHAR, cast later in dbt
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
        TO_VARIANT($1:payment_types),     -- array → VARIANT (not PARSE_JSON)
        $1:avg_review_score::FLOAT,
        $1:delivery_days::INTEGER,
        $1:is_late_delivery::INTEGER,
        TO_VARCHAR(                       -- derive from timestamp, not from file
            DATE_TRUNC('month', $1:order_purchase_timestamp::TIMESTAMP_NTZ),
            'YYYY-MM'
        )
    FROM @OLIST_DW.RAW.olist_processed_stage/fct_orders/
)
FILE_FORMAT = (TYPE=PARQUET SNAPPY_COMPRESSION=TRUE)
FORCE = TRUE;

-- ── Verify ────────────────────────────────────────────────────
SELECT 'dim_customers' AS table_name, COUNT(*) AS rows FROM OLIST_DW.RAW.dim_customers
UNION ALL SELECT 'dim_products',      COUNT(*) FROM OLIST_DW.RAW.dim_products
UNION ALL SELECT 'dim_sellers',       COUNT(*) FROM OLIST_DW.RAW.dim_sellers
UNION ALL SELECT 'fct_orders',        COUNT(*) FROM OLIST_DW.RAW.fct_orders
ORDER BY table_name;
EOF

echo "02_create_external_stage.sql updated ✅"