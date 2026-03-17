select
    metric_date as report_date,
    customer_token,
    customer_status,
    city,
    state,
    views,
    ad_clicks,
    add_to_cart,
    checkout_starts,
    purchases,
    order_amount,
    avg_order_value,
    view_to_purchase_rate,
    click_to_purchase_rate,
    cart_to_purchase_rate
from {{ ref('mart_customer_conversion') }}
