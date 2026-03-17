{{ config(partition_by=['order_date']) }}

select
    order_id,
    {{ tokenize_identifier('customer_id') }} as customer_token,
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
from {{ ref('stg_silver_order_header') }}
