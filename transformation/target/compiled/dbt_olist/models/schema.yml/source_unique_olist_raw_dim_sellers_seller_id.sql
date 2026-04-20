
    
    

select
    seller_id as unique_field,
    count(*) as n_records

from OLIST_DW.RAW.dim_sellers
where seller_id is not null
group by seller_id
having count(*) > 1


