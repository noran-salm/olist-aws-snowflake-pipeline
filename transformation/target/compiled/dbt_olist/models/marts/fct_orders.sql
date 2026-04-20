

WITH orders AS (
    SELECT * FROM OLIST_DW.STAGING.stg_orders
    
    WHERE order_purchase_timestamp > (
        SELECT COALESCE(MAX(order_purchase_timestamp), '2000-01-01')
        FROM OLIST_DW.MARTS.fct_orders
    )
    
),
customers AS (
    SELECT customer_id, city AS customer_city,
           state AS customer_state, customer_segment
    FROM OLIST_DW.MARTS.dim_customers
),
products AS (
    SELECT product_id, category,
           revenue_tier AS product_revenue_tier
    FROM OLIST_DW.MARTS.dim_products
),
sellers AS (
    SELECT seller_id, city AS seller_city,
           state AS seller_state, seller_tier
    FROM OLIST_DW.MARTS.dim_sellers
)

SELECT
    o.order_id,
    o.order_item_id,
    o.customer_id,
    o.seller_id,
    o.product_id,
    c.customer_city,
    c.customer_state,
    c.customer_segment,
    p.category                      AS product_category,
    p.product_revenue_tier,
    s.seller_city,
    s.seller_state,
    s.seller_tier,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.shipping_limit_date,
    o.order_year_month,
    o.item_price,
    o.freight_value,
    o.order_item_revenue,
    o.total_payment_value,
    o.max_installments,
    o.payment_types,
    o.avg_review_score,
    o.delivery_days,
    o.is_late_delivery,
    DATE_TRUNC('month',   o.order_purchase_timestamp) AS order_month,
    DATE_TRUNC('quarter', o.order_purchase_timestamp) AS order_quarter,
    YEAR(o.order_purchase_timestamp)                  AS order_year,
    CURRENT_TIMESTAMP()                               AS updated_at
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products  p ON o.product_id  = p.product_id
LEFT JOIN sellers   s ON o.seller_id   = s.seller_id