select
    metric_date,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(attributed_orders) as attributed_orders,
    sum(attributed_revenue) as attributed_revenue
from iceberg.gold.mart_campaign_performance
group by 1
order by 1
