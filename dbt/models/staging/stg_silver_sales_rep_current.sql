select
    sales_rep_id,
    rep_name,
    team_name,
    region,
    manager_name,
    status,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts
from {{ source('silver', 'silver_sales_rep_current') }}
