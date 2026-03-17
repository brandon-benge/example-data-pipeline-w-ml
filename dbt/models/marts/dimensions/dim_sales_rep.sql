select
    sales_rep_id,
    rep_name,
    team_name,
    region,
    manager_name,
    status as sales_rep_status,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts,
    current_timestamp() as dbt_loaded_at
from {{ ref('stg_silver_sales_rep_current') }}
