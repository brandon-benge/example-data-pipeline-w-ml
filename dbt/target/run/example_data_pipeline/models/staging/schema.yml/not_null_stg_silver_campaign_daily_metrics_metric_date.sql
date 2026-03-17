
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select metric_date
from gold.stg_silver_campaign_daily_metrics
where metric_date is null



  
  
      
    ) dbt_internal_test