
  
    
        create or replace table gold.fct_advertiser_daily
      
      
    using iceberg
      
      
      partitioned by (metric_date)
      
      
      

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
    processed_ts,
    current_timestamp() as dbt_loaded_at
from gold.stg_silver_advertiser_daily_metrics
where advertiser_id is not null
  