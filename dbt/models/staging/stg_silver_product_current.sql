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
from {{ source('silver', 'silver_product_current') }}
