
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- tests/assert_monthly_revenue_completeness.sql
-- Ensures 2017 and 2018 data exists (our known date range)

WITH monthly_check AS (
    SELECT
        order_year_month,
        SUM(revenue_brl) AS revenue
    FROM OLIST_DW.MARTS.fct_monthly_revenue
    WHERE order_year_month BETWEEN '2017-01' AND '2018-08'
    GROUP BY 1
    HAVING SUM(revenue_brl) < 1000  -- any month < R$1000 is suspicious
)
SELECT * FROM monthly_check
  
  
      
    ) dbt_internal_test