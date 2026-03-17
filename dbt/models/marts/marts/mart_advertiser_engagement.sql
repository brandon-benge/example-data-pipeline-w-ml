{{ config(partition_by=['metric_date']) }}

select
    f.metric_date,
    f.advertiser_id,
    a.advertiser_name,
    a.industry,
    a.account_tier,
    a.region,
    a.owner_sales_rep_id,
    sr.rep_name as owner_sales_rep_name,
    f.active_campaigns,
    f.sales_contacts,
    f.impressions,
    f.clicks,
    {{ safe_divide('least(f.clicks, f.impressions)', 'f.impressions') }} as ctr,
    f.attributed_orders,
    cast(f.attributed_revenue as decimal(18,2)) as attributed_revenue,
    {{ safe_divide('f.attributed_revenue', 'f.sales_contacts') }} as revenue_per_sales_contact,
    current_timestamp() as dbt_loaded_at
from {{ ref('fct_advertiser_daily') }} as f
left join {{ ref('dim_advertiser') }} as a
    on f.advertiser_id = a.advertiser_id
left join {{ ref('dim_sales_rep') }} as sr
    on a.owner_sales_rep_id = sr.sales_rep_id
