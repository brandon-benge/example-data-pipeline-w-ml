
  
    
        create or replace table gold.mart_advertiser_engagement
      
      
    using iceberg
      
      
      partitioned by (metric_date)
      
      
      

      as
      

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
    case
    when f.impressions is null or f.impressions = 0 then 0.0
    else cast(least(f.clicks, f.impressions) as double) / cast(f.impressions as double)
end as ctr,
    f.attributed_orders,
    cast(f.attributed_revenue as decimal(18,2)) as attributed_revenue,
    case
    when f.sales_contacts is null or f.sales_contacts = 0 then 0.0
    else cast(f.attributed_revenue as double) / cast(f.sales_contacts as double)
end as revenue_per_sales_contact,
    current_timestamp() as dbt_loaded_at
from gold.fct_advertiser_daily as f
left join gold.dim_advertiser as a
    on f.advertiser_id = a.advertiser_id
left join gold.dim_sales_rep as sr
    on a.owner_sales_rep_id = sr.sales_rep_id
  