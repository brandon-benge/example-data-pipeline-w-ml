select
    metric_date,
    customer_id,
    views,
    ad_clicks,
    add_to_cart,
    checkout_starts,
    purchases,
    order_amount,
    avg_order_value,
    processed_ts
from silver.silver_customer_daily_metrics