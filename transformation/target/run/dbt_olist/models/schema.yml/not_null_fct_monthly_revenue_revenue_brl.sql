
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select revenue_brl
from OLIST_DW.MARTS.fct_monthly_revenue
where revenue_brl is null



  
  
      
    ) dbt_internal_test