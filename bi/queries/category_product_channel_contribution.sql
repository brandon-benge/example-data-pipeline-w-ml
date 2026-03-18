with product_views as (
    select
        event_date as metric_date,
        customer_token,
        product_id,
        count(*) as product_views
    from iceberg.gold.fct_session_events
    where event_type = 'product_view'
      and product_id is not null
    group by 1, 2, 3
),
channel_candidates as (
    select
        oi.order_id,
        e.channel,
        row_number() over (
            partition by oi.order_id
            order by
                case when e.product_id = oi.product_id then 0 else 1 end,
                date_diff('day', e.event_date, oi.order_date),
                e.event_ts desc
        ) as channel_rank
    from iceberg.gold.fct_order_items as oi
    left join iceberg.gold.fct_session_events as e
        on e.customer_token = oi.customer_token
       and e.event_date <= oi.order_date
       and e.event_date >= date_add('day', -14, oi.order_date)
       and (e.product_id = oi.product_id or e.product_id is null)
       and e.channel is not null
),
orders_with_channel as (
    select
        oi.order_id,
        oi.order_date as metric_date,
        oi.customer_token,
        oi.product_id,
        oi.quantity,
        oi.line_amount,
        coalesce(ch.channel, 'unknown') as channel
    from iceberg.gold.fct_order_items as oi
    left join channel_candidates as ch
        on oi.order_id = ch.order_id
       and ch.channel_rank = 1
)
select
    oi.metric_date,
    oi.channel,
    dp.category,
    oi.product_id,
    dp.product_name,
    count(distinct oi.order_id) as orders,
    sum(oi.quantity) as quantity_sold,
    sum(oi.line_amount) as revenue,
    count(distinct oi.customer_token) as purchasing_customers,
    coalesce(sum(pv.product_views), 0) as product_views
from orders_with_channel as oi
inner join iceberg.gold.dim_product as dp
    on oi.product_id = dp.product_id
left join product_views as pv
    on oi.metric_date = pv.metric_date
   and oi.customer_token = pv.customer_token
   and oi.product_id = pv.product_id
group by 1, 2, 3, 4, 5
order by metric_date, revenue desc
