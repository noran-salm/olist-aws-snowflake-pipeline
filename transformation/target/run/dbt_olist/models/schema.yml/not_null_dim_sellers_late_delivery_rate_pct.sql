
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select late_delivery_rate_pct
from OLIST_DW.MARTS.dim_sellers
where late_delivery_rate_pct is null



  
  
      
    ) dbt_internal_test