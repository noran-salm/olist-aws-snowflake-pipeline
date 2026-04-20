
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_year_month
from OLIST_DW.MARTS.fct_monthly_revenue
where order_year_month is null



  
  
      
    ) dbt_internal_test