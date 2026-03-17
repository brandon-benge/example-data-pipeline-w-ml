
  
    
        create or replace table gold.fct_campaign_daily
      
      
    using iceberg
      
      
      partitioned by (metric_date)
      
      
      

      as
      

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
  