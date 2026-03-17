
  
    
        create or replace table gold.stg_silver_product_current
      
      
    using iceberg
      
      
      
      
      
      

      as
      select
    product_id,
    sku,
    product_name,
    brand,
    category,
    subcategory,
    list_price,
    cost,
    active_flag,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts
from silver.silver_product_current
  