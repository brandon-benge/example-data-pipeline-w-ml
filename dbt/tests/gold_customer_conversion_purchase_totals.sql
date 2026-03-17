with mart_totals as (
    select
        coalesce(sum(purchases), 0) as purchases,
        coalesce(sum(order_amount), cast(0 as decimal(18,2))) as order_amount
    from {{ ref('mart_customer_conversion') }}
),
fact_totals as (
    select
        coalesce(count(distinct order_id), 0) as purchases,
        coalesce(sum(total_amount), cast(0 as decimal(18,2))) as order_amount
    from {{ ref('fct_orders') }}
)
select
    mart_totals.purchases,
    mart_totals.order_amount,
    fact_totals.purchases as expected_purchases,
    fact_totals.order_amount as expected_order_amount
from mart_totals
cross join fact_totals
where mart_totals.purchases != fact_totals.purchases
   or mart_totals.order_amount != fact_totals.order_amount
