
  
    
        create or replace table gold.stg_silver_customer_session
      
      
    using iceberg
      
      
      
      
      
      

      as
      select
    session_id,
    customer_id,
    session_start_ts,
    session_end_ts,
    device_type,
    channel,
    referrer_type,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts
from silver.silver_customer_session
  