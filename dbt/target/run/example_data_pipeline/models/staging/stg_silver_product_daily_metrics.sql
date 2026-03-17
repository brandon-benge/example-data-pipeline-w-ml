
  
    
        create or replace table gold.stg_silver_product_daily_metrics
      
      
    using iceberg
      
      
      
      
      
      

      as
      select
    metric_date,
    product_id,
    product_views,
    add_to_cart,
    attributed_orders,
    attributed_revenue,
    processed_ts
from silver.silver_product_daily_metrics
  