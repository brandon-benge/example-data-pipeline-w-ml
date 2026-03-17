

select
    m.metric_date,
    sha2(concat('local-demo-tokenization-salt', '::', cast(m.customer_id as string)), 256) as customer_token,
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
    case
    when m.views is null or m.views = 0 then 0.0
    else cast(m.purchases as double) / cast(m.views as double)
end as view_to_purchase_rate,
    case
    when m.ad_clicks is null or m.ad_clicks = 0 then 0.0
    else cast(m.purchases as double) / cast(m.ad_clicks as double)
end as click_to_purchase_rate,
    case
    when m.add_to_cart is null or m.add_to_cart = 0 then 0.0
    else cast(m.purchases as double) / cast(m.add_to_cart as double)
end as cart_to_purchase_rate,
    current_timestamp() as dbt_loaded_at
from gold.stg_silver_customer_daily_metrics as m
left join gold.dim_customer as c
    on sha2(concat('local-demo-tokenization-salt', '::', cast(m.customer_id as string)), 256) = c.customer_token