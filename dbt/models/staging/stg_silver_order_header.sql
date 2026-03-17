select
    order_id,
    customer_id,
    order_ts,
    cast(order_ts as date) as order_date,
    order_status,
    subtotal_amount,
    discount_amount,
    tax_amount,
    total_amount,
    payment_type,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts
from {{ source('silver', 'silver_order_header') }}
