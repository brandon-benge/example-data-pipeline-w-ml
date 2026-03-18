# Ad Hoc Queries

This file documents the repo-managed SQL that backs the analytics demo.

## Source of truth

Superset is the runtime home for saved datasets, charts, and dashboards, but the query definitions still live in the repository under [bi/queries](../bi/queries).

Why they are still stored on disk:

- Superset metadata lives in an application database and is not a good code-review surface by itself.
- Repo-managed SQL is easier to diff, test, and keep aligned with dashboards, datasets, and Trino changes.
- The Superset bootstrap process imports these files so local environments start from the same approved definitions.

So the practical model is:

- Superset is the serving surface.
- `bi/queries/` is the version-controlled source of truth.
- This document is a lightweight guide, not a second independent query catalog.

## Scope

- Use Trino for ad hoc SQL.
- Default to curated Gold datasets in `iceberg.gold`.
- Treat Silver as an engineering layer unless you are validating upstream behavior.

## Preferred datasets

Primary marts:

- `iceberg.gold.mart_campaign_performance`
- `iceberg.gold.mart_advertiser_engagement`
- `iceberg.gold.mart_customer_conversion`

Supporting facts and dimensions:

- `iceberg.gold.fct_session_events`
- `iceberg.gold.fct_orders`
- `iceberg.gold.fct_order_items`
- `iceberg.gold.fct_sales_activity`
- `iceberg.gold.fct_campaign_daily`
- `iceberg.gold.fct_advertiser_daily`
- `iceberg.gold.dim_customer`
- `iceberg.gold.dim_product`
- `iceberg.gold.dim_advertiser`
- `iceberg.gold.dim_sales_rep`
- `iceberg.gold.dim_campaign`
- `iceberg.gold.dim_date`

## Query examples

Campaign performance over time:

```sql
select
    metric_date,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(attributed_orders) as attributed_orders,
    sum(attributed_revenue) as attributed_revenue
from iceberg.gold.mart_campaign_performance
group by 1
order by 1
```

Advertiser engagement effectiveness:

```sql
select
    metric_date,
    advertiser_name,
    account_tier,
    active_campaigns,
    sales_contacts,
    attributed_revenue,
    revenue_per_sales_contact
from iceberg.gold.mart_advertiser_engagement
order by metric_date, advertiser_name
```

Customer browse-to-purchase funnel:

```sql
with daily_funnel as (
    select
        metric_date,
        sum(views) as views,
        sum(ad_clicks) as ad_clicks,
        sum(add_to_cart) as add_to_cart,
        sum(checkout_starts) as checkout_starts,
        sum(purchases) as purchases
    from iceberg.gold.mart_customer_conversion
    group by 1
)
select metric_date, 'views' as stage_name, views as stage_total from daily_funnel
union all
select metric_date, 'ad_clicks' as stage_name, ad_clicks as stage_total from daily_funnel
union all
select metric_date, 'add_to_cart' as stage_name, add_to_cart as stage_total from daily_funnel
union all
select metric_date, 'checkout_starts' as stage_name, checkout_starts as stage_total from daily_funnel
union all
select metric_date, 'purchases' as stage_name, purchases as stage_total from daily_funnel
```

Category, product, and channel contribution:

```sql
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
```

## Operational notes

- Trino is exposed locally on `http://localhost:8080`.
- Superset uses the same Trino endpoint and defaults BI assets to the `gold` schema.
- The runtime-imported query JSON and SQL live under [bi/](../bi), not in this markdown file.
