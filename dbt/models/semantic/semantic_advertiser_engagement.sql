select
    metric_date as report_date,
    advertiser_id,
    advertiser_name,
    industry,
    account_tier,
    region,
    owner_sales_rep_name,
    active_campaigns,
    sales_contacts,
    impressions,
    clicks,
    ctr as click_through_rate,
    attributed_orders,
    attributed_revenue,
    revenue_per_sales_contact
from {{ ref('mart_advertiser_engagement') }}
