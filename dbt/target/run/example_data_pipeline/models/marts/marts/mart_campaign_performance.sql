
  
    
        create or replace table gold.mart_campaign_performance
      
      
    using iceberg
      
      
      partitioned by (metric_date)
      
      
      

      as
      

select
    f.metric_date,
    f.campaign_id,
    f.advertiser_id,
    f.impressions,
    f.clicks,
    case
    when f.impressions is null or f.impressions = 0 then 0.0
    else cast(least(f.clicks, f.impressions) as double) / cast(f.impressions as double)
end as ctr,
    f.attributed_orders,
    cast(f.attributed_revenue as decimal(18,2)) as attributed_revenue,
    f.sales_contacts,
    d.budget_amount,
    d.campaign_status,
    current_timestamp() as dbt_loaded_at
from gold.fct_campaign_daily as f
left join gold.dim_campaign as d
    on f.campaign_id = d.campaign_id
  