-- models/staging/stg_orders.sql
WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'fct_orders') }}
),
renamed AS (
    SELECT
        order_id,
        TRY_CAST(order_item_id AS INTEGER)          AS order_item_id,
        customer_id,
        seller_id,
        product_id,
        LOWER(TRIM(order_status))                   AS order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        shipping_limit_date,
        price                                       AS item_price,
        freight_value,
        order_item_revenue,
        total_payment_value,
        max_installments,
        payment_types,
        ROUND(avg_review_score, 2)                  AS avg_review_score,
        delivery_days,
        is_late_delivery::BOOLEAN                   AS is_late_delivery,
        order_year_month
    FROM source
    WHERE order_id IS NOT NULL
      AND order_purchase_timestamp >= '{{ var("start_date") }}'
)
SELECT * FROM renamed
