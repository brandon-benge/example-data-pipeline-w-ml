

select
    metric_date,
    campaign_id,
    advertiser_id,
    impressions,
    clicks,
    attributed_orders,
    attributed_revenue,
    sales_contacts,
    processed_ts,
    current_timestamp() as dbt_loaded_at
from gold.stg_silver_campaign_daily_metrics