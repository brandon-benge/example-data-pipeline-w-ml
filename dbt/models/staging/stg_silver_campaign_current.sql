select
    campaign_id,
    advertiser_id,
    campaign_name,
    campaign_type,
    objective,
    budget_amount,
    start_date,
    end_date,
    status,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts
from {{ source('silver', 'silver_campaign_current') }}
