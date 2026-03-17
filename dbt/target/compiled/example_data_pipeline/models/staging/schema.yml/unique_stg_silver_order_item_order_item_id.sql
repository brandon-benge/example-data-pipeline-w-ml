
    
    

select
    order_item_id as unique_field,
    count(*) as n_records

from gold.stg_silver_order_item
where order_item_id is not null
group by order_item_id
having count(*) > 1


