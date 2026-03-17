with as_of_rows as (
    select distinct
        campaign_id,
        advertiser_id,
        metric_date as as_of_date
    from {{ ref('stg_silver_campaign_daily_metrics') }}
)
select
    a.as_of_date,
    a.campaign_id as entity_id,
    a.advertiser_id,
    cast(sum(case when datediff(a.as_of_date, d.metric_date) between 0 and 6 then d.impressions else 0 end) as bigint) as impressions_7d,
    cast(sum(case when datediff(a.as_of_date, d.metric_date) between 0 and 6 then d.clicks else 0 end) as bigint) as clicks_7d,
    coalesce(
        cast(sum(case when datediff(a.as_of_date, d.metric_date) between 0 and 6 then d.clicks else 0 end) as double) /
        nullif(cast(sum(case when datediff(a.as_of_date, d.metric_date) between 0 and 6 then d.impressions else 0 end) as double), 0.0),
        0.0
    ) as ctr_7d,
    cast(sum(case when datediff(a.as_of_date, d.metric_date) between 0 and 29 then d.attributed_orders else 0 end) as bigint) as attributed_orders_30d,
    cast(sum(case when datediff(a.as_of_date, d.metric_date) between 0 and 29 then d.attributed_revenue else 0 end) as double) as attributed_revenue_30d,
    'campaign' as feature_group,
    'campaign_success_features_v1' as feature_definition_version,
    'campaign_success_flag' as label_name,
    cast(
        max(
            case
                when d.metric_date > a.as_of_date
                 and d.metric_date <= date_add(a.as_of_date, 7)
                 and (
                    d.attributed_orders > 0
                    or coalesce(cast(d.clicks as double) / nullif(cast(d.impressions as double), 0.0), 0.0) >= 0.02
                 )
                then 1
                else 0
            end
        ) as int
    ) as label_value,
    current_timestamp() as generated_ts
from as_of_rows as a
join {{ ref('stg_silver_campaign_daily_metrics') }} as d
    on a.campaign_id = d.campaign_id
group by a.as_of_date, a.campaign_id, a.advertiser_id
