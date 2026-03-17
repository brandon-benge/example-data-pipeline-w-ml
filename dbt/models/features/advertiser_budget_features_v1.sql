with budget_history as (
    select
        metric_date,
        advertiser_id,
        cast(active_campaigns * 1000 as double) as total_budget_amount
    from {{ ref('stg_silver_advertiser_daily_metrics') }}
),
as_of_rows as (
    select distinct
        advertiser_id,
        metric_date as as_of_date
    from {{ ref('stg_silver_advertiser_daily_metrics') }}
)
select
    a.as_of_date,
    a.advertiser_id as entity_id,
    cast(max(case when datediff(a.as_of_date, d.metric_date) between 0 and 29 then d.active_campaigns else 0 end) as bigint) as active_campaigns_30d,
    cast(sum(case when datediff(a.as_of_date, d.metric_date) between 0 and 13 then d.sales_contacts else 0 end) as bigint) as sales_contacts_14d,
    cast(
        coalesce(max(case when b.metric_date > a.as_of_date and b.metric_date <= date_add(a.as_of_date, 30) then b.total_budget_amount end), 0.0) -
        coalesce(max(case when b.metric_date <= a.as_of_date then b.total_budget_amount end), 0.0)
        as double
    ) as budget_delta_30d,
    cast(sum(case when datediff(a.as_of_date, d.metric_date) between 0 and 6 then d.impressions else 0 end) as bigint) as impressions_7d,
    cast(sum(case when datediff(a.as_of_date, d.metric_date) between 0 and 6 then d.clicks else 0 end) as bigint) as clicks_7d,
    cast(sum(case when datediff(a.as_of_date, d.metric_date) between 0 and 29 then d.attributed_revenue else 0 end) as double) as attributed_revenue_30d,
    'advertiser' as feature_group,
    'advertiser_budget_features_v1' as feature_definition_version,
    'advertiser_budget_increase_next_30d' as label_name,
    cast(
        case
            when (
                coalesce(max(case when b.metric_date > a.as_of_date and b.metric_date <= date_add(a.as_of_date, 30) then b.total_budget_amount end), 0.0) -
                coalesce(max(case when b.metric_date <= a.as_of_date then b.total_budget_amount end), 0.0)
            ) > 0 then 1
            else 0
        end as int
    ) as label_value,
    current_timestamp() as generated_ts
from as_of_rows as a
join {{ ref('stg_silver_advertiser_daily_metrics') }} as d
    on a.advertiser_id = d.advertiser_id
left join budget_history as b
    on a.advertiser_id = b.advertiser_id
group by a.as_of_date, a.advertiser_id
