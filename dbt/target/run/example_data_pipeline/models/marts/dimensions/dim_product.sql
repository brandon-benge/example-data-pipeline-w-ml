
  
    
        create or replace table gold.dim_product
      
      
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
    silver_processed_ts,
    current_timestamp() as dbt_loaded_at
from gold.stg_silver_product_current
  