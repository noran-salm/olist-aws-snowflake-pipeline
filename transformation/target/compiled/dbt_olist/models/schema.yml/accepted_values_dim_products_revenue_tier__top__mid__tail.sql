
    
    

with all_values as (

    select
        revenue_tier as value_field,
        count(*) as n_records

    from OLIST_DW.MARTS.dim_products
    group by revenue_tier

)

select *
from all_values
where value_field not in (
    'top','mid','tail'
)


