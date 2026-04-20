
    
    

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


