with daily_funnel as (
    select
        metric_date,
        sum(views) as views,
        sum(ad_clicks) as ad_clicks,
        sum(add_to_cart) as add_to_cart,
        sum(checkout_starts) as checkout_starts,
        sum(purchases) as purchases
    from lakehouse.gold.mart_customer_conversion
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
