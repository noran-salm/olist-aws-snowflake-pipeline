USE ROLE SYSADMIN;
USE WAREHOUSE OLIST_WH;

-- ── Row counts in RAW ─────────────────────────────────────────
SELECT 'RAW.dim_customers' AS layer_table, COUNT(*) AS raws FROM OLIST_DW.RAW.dim_customers
UNION ALL SELECT 'RAW.dim_products',  COUNT(*) FROM OLIST_DW.RAW.dim_products
UNION ALL SELECT 'RAW.dim_sellers',   COUNT(*) FROM OLIST_DW.RAW.dim_sellers
UNION ALL SELECT 'RAW.fct_orders',    COUNT(*) FROM OLIST_DW.RAW.fct_orders

-- ── Row counts in MARTS ───────────────────────────────────────
UNION ALL SELECT 'MARTS.dim_customers', COUNT(*) FROM OLIST_DW.MARTS.dim_customers
UNION ALL SELECT 'MARTS.dim_products',  COUNT(*) FROM OLIST_DW.MARTS.dim_products
UNION ALL SELECT 'MARTS.dim_sellers',   COUNT(*) FROM OLIST_DW.MARTS.dim_sellers
UNION ALL SELECT 'MARTS.fct_orders',    COUNT(*) FROM OLIST_DW.MARTS.fct_orders
ORDER BY layer_table;

-- ── Quick business sense check ────────────────────────────────
SELECT
    order_year_month,
    COUNT(DISTINCT order_id)         AS orders,
    ROUND(SUM(order_item_revenue),0) AS revenue_brl,
    ROUND(AVG(avg_review_score),2)   AS avg_score
FROM OLIST_DW.MARTS.fct_orders
WHERE order_year_month IS NOT NULL
GROUP BY 1
ORDER BY 1;