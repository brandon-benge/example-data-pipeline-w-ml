{{ config(partition_by=['metric_date']) }}

select
    m.metric_date,
    {{ tokenize_identifier('m.customer_id') }} as customer_token,
    c.customer_status,
    c.city,
    c.state,
    m.views,
    m.ad_clicks,
    m.add_to_cart,
    m.checkout_starts,
    m.purchases,
    cast(m.order_amount as decimal(18,2)) as order_amount,
    cast(m.avg_order_value as double) as avg_order_value,
    {{ safe_divide('m.purchases', 'm.views') }} as view_to_purchase_rate,
    {{ safe_divide('m.purchases', 'm.ad_clicks') }} as click_to_purchase_rate,
    {{ safe_divide('m.purchases', 'm.add_to_cart') }} as cart_to_purchase_rate,
    current_timestamp() as dbt_loaded_at
from {{ ref('stg_silver_customer_daily_metrics') }} as m
left join {{ ref('dim_customer') }} as c
    on {{ tokenize_identifier('m.customer_id') }} = c.customer_token
