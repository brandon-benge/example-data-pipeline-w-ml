select
    advertiser_id,
    advertiser_name,
    industry,
    account_tier,
    region,
    owner_sales_rep_id,
    status as advertiser_status,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts,
    current_timestamp() as dbt_loaded_at
from gold.stg_silver_advertiser_current