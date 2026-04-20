-- models/marts/dim_sellers.sql
-- Final seller dimension enriched with GMV and performance KPIs.

WITH sellers AS (
    SELECT * FROM OLIST_DW.STAGING.stg_sellers
),

seller_stats AS (
    SELECT
        seller_id,
        COUNT(DISTINCT order_id)            AS total_orders,
        COUNT(*)                            AS total_items_sold,
        SUM(order_item_revenue)             AS total_gmv,
        AVG(item_price)                     AS avg_item_price,
        AVG(avg_review_score)               AS avg_review_score,
        AVG(delivery_days)                  AS avg_delivery_days,
        SUM(is_late_delivery::INTEGER)      AS late_deliveries,
        COUNT(DISTINCT product_id)          AS distinct_products_sold,
        MIN(order_purchase_timestamp)       AS first_sale_date,
        MAX(order_purchase_timestamp)       AS last_sale_date
    FROM OLIST_DW.STAGING.stg_orders
    WHERE order_status = 'delivered'
      AND seller_id IS NOT NULL
    GROUP BY 1
)

SELECT
    s.seller_id,
    s.zip_code_prefix,
    s.city,
    s.state,

    -- Performance KPIs
    COALESCE(st.total_orders,             0)  AS total_orders,
    COALESCE(st.total_items_sold,         0)  AS total_items_sold,
    COALESCE(st.total_gmv,                0)  AS total_gmv,
    COALESCE(st.avg_item_price,           0)  AS avg_item_price,
    COALESCE(st.avg_review_score,         0)  AS avg_review_score,
    COALESCE(st.avg_delivery_days,        0)  AS avg_delivery_days,
    COALESCE(st.late_deliveries,          0)  AS late_deliveries,
    COALESCE(st.distinct_products_sold,   0)  AS distinct_products_sold,

    -- Late delivery rate (%)
    CASE
        WHEN COALESCE(st.total_orders, 0) = 0 THEN 0
        ELSE ROUND(100.0 * st.late_deliveries / st.total_orders, 2)
    END AS late_delivery_rate_pct,

    -- Seller tier by GMV
    CASE
        WHEN COALESCE(st.total_gmv, 0) >= 50000 THEN 'platinum'
        WHEN COALESCE(st.total_gmv, 0) >= 10000 THEN 'gold'
        WHEN COALESCE(st.total_gmv, 0) >= 1000  THEN 'silver'
        ELSE                                          'bronze'
    END AS seller_tier,

    st.first_sale_date,
    st.last_sale_date,
    CURRENT_TIMESTAMP() AS updated_at

FROM sellers s
LEFT JOIN seller_stats st ON s.seller_id = st.seller_id