

WITH orders AS (
    SELECT * FROM OLIST_DW.MARTS.fct_orders
    
    WHERE order_year_month > (
        SELECT COALESCE(MAX(order_year_month), '2000-01')
        FROM OLIST_DW.MARTS.fct_monthly_revenue
    )
    
),
states AS (SELECT * FROM OLIST_DW.MARTS.brazil_states)

SELECT
    o.order_year_month,
    o.product_category,
    o.customer_state,
    s.state_name                               AS customer_state_name,
    s.region                                   AS customer_region,
    o.seller_state,
    COUNT(DISTINCT o.order_id)                 AS total_orders,
    COUNT(*)                                   AS total_items,
    ROUND(SUM(o.order_item_revenue), 2)        AS revenue_brl,
    ROUND(AVG(o.avg_review_score), 2)          AS avg_review,
    SUM(o.is_late_delivery::INTEGER)           AS late_deliveries,
    ROUND(AVG(o.delivery_days), 1)             AS avg_delivery_days
FROM orders o
LEFT JOIN states s ON o.customer_state = s.state_code
WHERE o.order_year_month IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6