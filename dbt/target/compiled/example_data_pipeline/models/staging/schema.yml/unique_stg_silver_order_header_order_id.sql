
    
    

select
    order_id as unique_field,
    count(*) as n_records

from gold.stg_silver_order_header
where order_id is not null
group by order_id
having count(*) > 1


