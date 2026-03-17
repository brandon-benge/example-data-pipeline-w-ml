with order_stats as (
    select
        d.customer_id,
        d.metric_date as as_of_date,
        sum(case when datediff(d.metric_date, cast(o.order_ts as date)) between 0 and 29 then 1 else 0 end) as purchases_30d,
        avg(case when datediff(d.metric_date, cast(o.order_ts as date)) between 0 and 89 then o.total_amount end) as avg_order_value_90d,
        coalesce(
            datediff(
                d.metric_date,
                max(case when cast(o.order_ts as date) <= d.metric_date then cast(o.order_ts as date) end)
            ),
            9999
        ) as days_since_last_purchase,
        max(
            case
                when cast(o.order_ts as date) > d.metric_date
                 and cast(o.order_ts as date) <= date_add(d.metric_date, 7)
                then 1
                else 0
            end
        ) as label_value
    from {{ ref('stg_silver_customer_daily_metrics') }} as d
    left join {{ ref('stg_silver_order_header') }} as o
        on d.customer_id = o.customer_id
    group by d.customer_id, d.metric_date
)
select
    cast(as_of_date as date) as as_of_date,
    {{ tokenize_identifier('customer_id') }} as customer_token,
    cast(coalesce(purchases_30d, 0) as bigint) as purchases_30d,
    cast(coalesce(avg_order_value_90d, 0.0) as double) as avg_order_value_30d,
    cast(coalesce(label_value, 0) as int) as customer_purchase_next_7d,
    'customer_purchase_features_v1' as feature_version,
    current_timestamp() as generated_ts
from order_stats
