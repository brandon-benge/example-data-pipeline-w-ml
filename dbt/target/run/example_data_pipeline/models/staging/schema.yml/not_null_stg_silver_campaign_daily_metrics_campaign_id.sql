
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select campaign_id
from gold.stg_silver_campaign_daily_metrics
where campaign_id is null



  
  
      
    ) dbt_internal_test