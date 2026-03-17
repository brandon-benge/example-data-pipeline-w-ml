# Ad Hoc Queries

This repository uses Trino as the ad hoc SQL entrypoint for curated Gold datasets.

## Scope

- Query Gold only by default.
- Use Trino against the `iceberg.gold` namespace.
- Treat Silver as an engineering/validation layer, not the default BI exploration layer.

## Preferred datasets

Primary curated marts:

- `iceberg.gold.mart_campaign_performance`
- `iceberg.gold.mart_advertiser_engagement`
- `iceberg.gold.mart_customer_conversion`

Supporting Gold facts and dimensions:

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

## Access expectations

- Gold is the business-facing layer for dashboards and ad hoc SQL.
- Broad-access customer-facing outputs must remain masked or tokenized.
- Use `customer_token` for customer-level joins in Gold; do not expect raw customer identifiers in broad-access Gold models.

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
order by 1;
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
order by metric_date, advertiser_name;
```

Customer browse-to-purchase funnel:

```sql
select
    metric_date,
    sum(views) as views,
    sum(ad_clicks) as ad_clicks,
    sum(add_to_cart) as add_to_cart,
    sum(checkout_starts) as checkout_starts,
    sum(purchases) as purchases
from iceberg.gold.mart_customer_conversion
group by 1
order by 1;
```

Category, product, and channel contribution:

```sql
with latest_channel as (
    select
        event_date as metric_date,
        customer_token,
        coalesce(channel, 'unknown') as channel,
        row_number() over (
            partition by event_date, customer_token
            order by event_ts desc
        ) as rn
    from iceberg.gold.fct_session_events
),
channel_by_customer_day as (
    select metric_date, customer_token, channel
    from latest_channel
    where rn = 1
)
select
    oi.order_date as metric_date,
    coalesce(ch.channel, 'unknown') as channel,
    dp.category,
    dp.product_name,
    count(distinct oi.order_id) as orders,
    sum(oi.line_amount) as revenue
from iceberg.gold.fct_order_items as oi
inner join iceberg.gold.dim_product as dp
    on oi.product_id = dp.product_id
left join channel_by_customer_day as ch
    on oi.order_date = ch.metric_date
   and oi.customer_token = ch.customer_token
group by 1, 2, 3, 4
order by metric_date, revenue desc;
```

## Operational notes

- Trino is exposed locally on `http://localhost:8080`.
- Superset uses the same Trino endpoint and defaults BI assets to the `gold` schema.
- Repository-managed saved query definitions also exist under [bi/queries](../bi/queries).
