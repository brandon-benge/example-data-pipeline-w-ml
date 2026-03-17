{{ config(partition_by=['order_date']) }}

select
    oi.order_item_id,
    oi.order_id,
    oh.order_date,
    oh.order_ts,
    {{ tokenize_identifier('oh.customer_id') }} as customer_token,
    oi.product_id,
    oi.attributed_campaign_id as campaign_id,
    oi.quantity,
    oi.unit_price,
    oi.line_amount,
    oh.order_status,
    oh.payment_type,
    oi.created_at,
    oi.updated_at,
    oi.source_last_change_ts,
    oi.silver_processed_ts,
    current_timestamp() as dbt_loaded_at
from {{ ref('stg_silver_order_item') }} as oi
inner join {{ ref('stg_silver_order_header') }} as oh
    on oi.order_id = oh.order_id
