
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select campaign_id
from gold.fct_campaign_daily
where campaign_id is null



  
  
      
    ) dbt_internal_test