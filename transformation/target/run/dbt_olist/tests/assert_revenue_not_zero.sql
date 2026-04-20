
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- tests/assert_revenue_not_zero.sql
-- Catches ETL bugs that zero out financial columns

SELECT COUNT(*) AS zero_revenue_orders
FROM OLIST_DW.MARTS.fct_orders
WHERE order_status = 'delivered'
  AND order_item_revenue = 0
HAVING COUNT(*) > 100  -- allow small tolerance
  
  
      
    ) dbt_internal_test