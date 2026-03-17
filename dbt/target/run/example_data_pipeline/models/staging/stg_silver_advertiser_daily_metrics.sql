
  
    
        create or replace table gold.stg_silver_advertiser_daily_metrics
      
      
    using iceberg
      
      
      
      
      
      

      as
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
from silver.silver_advertiser_daily_metrics
  