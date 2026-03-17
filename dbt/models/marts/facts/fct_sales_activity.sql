{{ config(partition_by=['activity_date']) }}

select
    sales_activity_id,
    activity_date,
    activity_ts,
    advertiser_id,
    sales_rep_id,
    activity_type,
    activity_outcome,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts,
    current_timestamp() as dbt_loaded_at
from {{ ref('stg_silver_sales_activity') }}
