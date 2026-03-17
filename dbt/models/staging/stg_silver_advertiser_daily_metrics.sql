select
    metric_date,
    advertiser_id,
    active_campaigns,
    sales_contacts,
    impressions,
    clicks,
    attributed_orders,
    attributed_revenue,
    processed_ts
from {{ source('silver', 'silver_advertiser_daily_metrics') }}
