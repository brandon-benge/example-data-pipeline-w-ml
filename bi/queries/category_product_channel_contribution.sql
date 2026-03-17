with product_views as (
    select
        event_date as metric_date,
        customer_token,
        product_id,
        count(*) as product_views
    from lakehouse.gold.fct_session_events
    where event_type = 'product_view'
      and product_id is not null
    group by 1, 2, 3
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
    from lakehouse.gold.fct_order_items as oi
    left join lateral (
        select e.channel
        from lakehouse.gold.fct_session_events as e
        where e.customer_token = oi.customer_token
          and e.event_date <= oi.order_date
          and e.channel is not null
        order by e.event_ts desc
        limit 1
    ) as ch on true
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
inner join lakehouse.gold.dim_product as dp
    on oi.product_id = dp.product_id
left join product_views as pv
    on oi.metric_date = pv.metric_date
   and oi.customer_token = pv.customer_token
   and oi.product_id = pv.product_id
group by 1, 2, 3, 4, 5
order by metric_date, revenue desc
