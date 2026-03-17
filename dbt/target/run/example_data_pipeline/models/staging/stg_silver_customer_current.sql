
  
    
        create or replace table gold.stg_silver_customer_current
      
      
    using iceberg
      
      
      
      
      
      

      as
      select
    customer_id,
    first_name_masked,
    last_name_masked,
    email_masked,
    phone_masked,
    city,
    state,
    zip_code_masked,
    status,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts
from silver.silver_customer_current
  