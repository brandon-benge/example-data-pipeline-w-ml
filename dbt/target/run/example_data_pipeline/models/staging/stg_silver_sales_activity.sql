
  
    
        create or replace table gold.stg_silver_sales_activity
      
      
    using iceberg
      
      
      
      
      
      

      as
      select
    sales_activity_id,
    advertiser_id,
    sales_rep_id,
    activity_ts,
    cast(activity_ts as date) as activity_date,
    activity_type,
    activity_outcome,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts
from silver.silver_sales_activity
  