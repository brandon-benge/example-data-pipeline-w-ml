select
    metric_date,
    campaign_id,
    advertiser_id,
    impressions,
    clicks,
    attributed_orders,
    attributed_revenue,
    sales_contacts,
    processed_ts
from {{ source('silver', 'silver_campaign_daily_metrics') }}
