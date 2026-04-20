
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select item_price
from OLIST_DW.STAGING.stg_orders
where item_price is null



  
  
      
    ) dbt_internal_test