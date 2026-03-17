
  
    
        create or replace table gold.dim_campaign
      
      
    using iceberg
      
      
      
      
      
      

      as
      select
    campaign_id,
    advertiser_id,
    campaign_name,
    campaign_type,
    objective,
    budget_amount,
    start_date,
    end_date,
    status as campaign_status,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts,
    current_timestamp() as dbt_loaded_at
from gold.stg_silver_campaign_current
  