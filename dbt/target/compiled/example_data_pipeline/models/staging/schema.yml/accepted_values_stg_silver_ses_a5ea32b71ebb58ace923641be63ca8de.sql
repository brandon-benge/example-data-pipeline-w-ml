
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from gold.stg_silver_session_event_clean
    group by event_type

)

select *
from all_values
where value_field not in (
    'product_view','ad_impression','ad_click','add_to_cart','checkout_start'
)


