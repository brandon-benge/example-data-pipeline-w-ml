
  
    
        create or replace table gold.dim_customer
      
      
    using iceberg
      
      
      
      
      
      

      as
      with customer_keys as (
    select customer_id
    from gold.stg_silver_customer_current

    union

    select customer_id
    from gold.stg_silver_customer_session
    where customer_id is not null

    union

    select customer_id
    from gold.stg_silver_session_event_clean
    where customer_id is not null
)

select
    sha2(concat('local-demo-tokenization-salt', '::', cast(k.customer_id as string)), 256) as customer_token,
    c.first_name_masked,
    c.last_name_masked,
    c.email_masked,
    c.phone_masked,
    c.city,
    c.state,
    c.zip_code_masked,
    c.status as customer_status,
    c.created_at,
    c.updated_at,
    c.source_last_change_ts,
    c.silver_processed_ts,
    current_timestamp() as dbt_loaded_at
from customer_keys as k
left join gold.stg_silver_customer_current as c
    on k.customer_id = c.customer_id
  