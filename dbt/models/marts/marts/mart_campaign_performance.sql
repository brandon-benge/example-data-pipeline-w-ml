{{ config(partition_by=['metric_date']) }}

select
    f.metric_date,
    f.campaign_id,
    f.advertiser_id,
    f.impressions,
    f.clicks,
    {{ safe_divide('least(f.clicks, f.impressions)', 'f.impressions') }} as ctr,
    f.attributed_orders,
    cast(f.attributed_revenue as decimal(18,2)) as attributed_revenue,
    f.sales_contacts,
    d.budget_amount,
    d.campaign_status,
    current_timestamp() as dbt_loaded_at
from {{ ref('fct_campaign_daily') }} as f
left join {{ ref('dim_campaign') }} as d
    on f.campaign_id = d.campaign_id
