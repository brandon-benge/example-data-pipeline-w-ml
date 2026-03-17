
  
    
        create or replace table silver.customer_purchase_realtime_features_v1
      
      
    using iceberg
      
      
      
      
      
      

      as
      with order_stats as (
    select
        d.customer_id,
        d.metric_date as as_of_date,
        sum(
            case
                when datediff(d.metric_date, cast(o.order_ts as date)) between 0 and 29 then 1
                else 0
            end
        ) as purchases_30d,
        avg(
            case
                when datediff(d.metric_date, cast(o.order_ts as date)) between 0 and 89 then o.total_amount
            end
        ) as avg_order_value_90d,
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
    from gold.stg_silver_customer_daily_metrics as d
    left join gold.stg_silver_order_header as o
        on d.customer_id = o.customer_id
    group by d.customer_id, d.metric_date
),
parity as (
    select
        customer_id,
        views_1h,
        views_24h,
        ad_clicks_24h,
        add_to_cart_24h,
        feature_version,
        last_event_ts,
        updated_at,
        ttl_seconds,
        row_number() over (partition by customer_id order by as_of_ts desc) as row_num
    from silver.customer_realtime_features_v1_parity
)
select
    cast(s.as_of_date as date) as as_of_date,
    cast(s.customer_id as bigint) as customer_id,
    cast(coalesce(p.views_1h, case when d.views > 0 then 1 else 0 end) as bigint) as views_1h,
    cast(coalesce(p.views_24h, d.views) as bigint) as views_24h,
    cast(coalesce(p.ad_clicks_24h, d.ad_clicks) as bigint) as ad_clicks_24h,
    cast(coalesce(p.add_to_cart_24h, d.add_to_cart) as bigint) as add_to_cart_24h,
    cast(coalesce(s.purchases_30d, 0) as bigint) as purchases_30d,
    cast(coalesce(s.avg_order_value_90d, 0.0) as double) as avg_order_value_90d,
    cast(coalesce(s.days_since_last_purchase, 9999) as int) as days_since_last_purchase,
    'customer_realtime' as feature_group,
    'customer_purchase_realtime_features_v1' as feature_definition_version,
    'customer_purchase_next_7d' as label_name,
    cast(coalesce(s.label_value, 0) as int) as label_value,
    coalesce(p.feature_version, 'customer_realtime_features_v1') as online_feature_version,
    p.last_event_ts,
    p.updated_at,
    cast(coalesce(p.ttl_seconds, 0) as int) as ttl_seconds,
    current_timestamp() as generated_ts
from order_stats as s
join gold.stg_silver_customer_daily_metrics as d
    on s.customer_id = d.customer_id
   and s.as_of_date = d.metric_date
left join parity as p
    on s.customer_id = p.customer_id
   and p.row_num = 1
  