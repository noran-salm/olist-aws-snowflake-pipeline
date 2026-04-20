-- models/marts/dim_products.sql
-- Final product dimension enriched with sales performance metrics.

WITH products AS (
    SELECT * FROM OLIST_DW.STAGING.stg_products
),

product_stats AS (
    SELECT
        product_id,
        COUNT(DISTINCT order_id)        AS total_orders,
        SUM(order_item_revenue)         AS total_revenue,
        AVG(item_price)                 AS avg_price,
        AVG(avg_review_score)           AS avg_review_score,
        SUM(is_late_delivery::INTEGER)  AS late_delivery_count,
        COUNT(*)                        AS total_items_sold
    FROM OLIST_DW.STAGING.stg_orders
    WHERE order_status != 'canceled'
    GROUP BY 1
)

SELECT
    p.product_id,
    p.category,
    p.name_length,
    p.description_length,
    p.photos_qty,
    p.product_weight_kg,
    p.volumetric_weight_kg,
    -- Use the larger of actual vs volumetric weight (courier billing logic)
    GREATEST(p.product_weight_kg, p.volumetric_weight_kg) AS billable_weight_kg,
    p.length_cm,
    p.height_cm,
    p.width_cm,

    -- Sales metrics
    COALESCE(s.total_orders,       0)   AS total_orders,
    COALESCE(s.total_items_sold,   0)   AS total_items_sold,
    COALESCE(s.total_revenue,      0)   AS total_revenue,
    COALESCE(s.avg_price,          0)   AS avg_price,
    COALESCE(s.avg_review_score,   0)   AS avg_review_score,
    COALESCE(s.late_delivery_count,0)   AS late_delivery_count,

    -- Performance tier
    CASE
        WHEN COALESCE(s.total_revenue, 0) >= 10000 THEN 'top'
        WHEN COALESCE(s.total_revenue, 0) >= 1000  THEN 'mid'
        ELSE                                             'tail'
    END AS revenue_tier,

    CURRENT_TIMESTAMP() AS updated_at

FROM products p
LEFT JOIN product_stats s ON p.product_id = s.product_id