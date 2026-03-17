select
    metric_date as report_date,
    campaign_id,
    advertiser_id,
    impressions,
    clicks,
    ctr as click_through_rate,
    attributed_orders,
    attributed_revenue,
    sales_contacts,
    budget_amount,
    campaign_status
from gold.mart_campaign_performance