with mart_totals as (
    select
        coalesce(sum(attributed_orders), 0) as attributed_orders,
        coalesce(sum(attributed_revenue), cast(0 as decimal(18,2))) as attributed_revenue
    from gold.mart_campaign_performance
),
fact_totals as (
    select
        coalesce(sum(attributed_orders), 0) as attributed_orders,
        coalesce(sum(attributed_revenue), cast(0 as decimal(18,2))) as attributed_revenue
    from gold.fct_campaign_daily
)
select
    mart_totals.attributed_orders,
    mart_totals.attributed_revenue,
    fact_totals.attributed_orders as expected_attributed_orders,
    fact_totals.attributed_revenue as expected_attributed_revenue
from mart_totals
cross join fact_totals
where mart_totals.attributed_orders != fact_totals.attributed_orders
   or mart_totals.attributed_revenue != fact_totals.attributed_revenue