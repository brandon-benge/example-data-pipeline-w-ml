select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_amount,
    attributed_campaign_id,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts
from {{ source('silver', 'silver_order_item') }}
