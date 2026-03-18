select
    metric_date,
    advertiser_name,
    account_tier,
    active_campaigns,
    sales_contacts,
    attributed_revenue,
    revenue_per_sales_contact
from iceberg.gold.mart_advertiser_engagement
order by metric_date, advertiser_name
