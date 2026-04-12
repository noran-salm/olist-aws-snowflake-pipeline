-- models/marts/dim_customers.sql
-- Final dimension table: one row per unique customer.
-- Enriched with order-level aggregates (lifetime value, order count).

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

-- Compute customer-level order aggregates from the staging orders view
order_stats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id)            AS total_orders,
        SUM(order_item_revenue)             AS lifetime_revenue,
        AVG(order_item_revenue)             AS avg_order_value,
        MIN(order_purchase_timestamp)       AS first_order_date,
        MAX(order_purchase_timestamp)       AS last_order_date,
        AVG(avg_review_score)               AS avg_review_score,
        SUM(is_late_delivery::INTEGER)      AS late_deliveries
    FROM {{ ref('stg_orders') }}
    WHERE order_status = 'delivered'
    GROUP BY 1
)

SELECT
    c.customer_id,
    c.customer_unique_id,
    c.zip_code_prefix,
    c.city,
    c.state,

    -- Order stats (NULL-safe: customer may have no delivered orders yet)
    COALESCE(o.total_orders,       0)          AS total_orders,
    COALESCE(o.lifetime_revenue,   0)          AS lifetime_revenue,
    COALESCE(o.avg_order_value,    0)          AS avg_order_value,
    o.first_order_date,
    o.last_order_date,
    COALESCE(o.avg_review_score,   0)          AS avg_review_score,
    COALESCE(o.late_deliveries,    0)          AS late_deliveries,

    -- Customer segment by lifetime value
    CASE
        WHEN COALESCE(o.lifetime_revenue, 0) >= 500  THEN 'high_value'
        WHEN COALESCE(o.lifetime_revenue, 0) >= 100  THEN 'mid_value'
        ELSE                                               'low_value'
    END AS customer_segment,

    -- Recency bucket (days since last order)
    DATEDIFF('day', o.last_order_date, CURRENT_DATE()) AS days_since_last_order,

    CURRENT_TIMESTAMP() AS updated_at

FROM customers c
LEFT JOIN order_stats o ON c.customer_id = o.customer_id
