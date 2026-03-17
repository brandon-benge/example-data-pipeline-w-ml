

select
    order_id,
    sha2(concat('local-demo-tokenization-salt', '::', cast(customer_id as string)), 256) as customer_token,
    order_date,
    order_ts,
    order_status,
    payment_type,
    subtotal_amount,
    discount_amount,
    tax_amount,
    total_amount,
    created_at,
    updated_at,
    source_last_change_ts,
    silver_processed_ts,
    current_timestamp() as dbt_loaded_at
from gold.stg_silver_order_header