
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        seller_tier as value_field,
        count(*) as n_records

    from OLIST_DW.MARTS.dim_sellers
    group by seller_tier

)

select *
from all_values
where value_field not in (
    'platinum','gold','silver','bronze'
)



  
  
      
    ) dbt_internal_test