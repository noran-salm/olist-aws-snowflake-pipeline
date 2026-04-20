
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- tests/assert_no_null_year_month.sql
-- Catches the Spark partition column issue we fixed earlier

SELECT COUNT(*) AS null_count
FROM OLIST_DW.MARTS.fct_orders
WHERE order_year_month IS NULL
HAVING COUNT(*) > 0
  
  
      
    ) dbt_internal_test