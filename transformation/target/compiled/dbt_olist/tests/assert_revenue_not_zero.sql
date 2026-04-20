-- tests/assert_revenue_not_zero.sql
-- Catches ETL bugs that zero out financial columns

SELECT COUNT(*) AS zero_revenue_orders
FROM OLIST_DW.MARTS.fct_orders
WHERE order_status = 'delivered'
  AND order_item_revenue = 0
HAVING COUNT(*) > 100  -- allow small tolerance