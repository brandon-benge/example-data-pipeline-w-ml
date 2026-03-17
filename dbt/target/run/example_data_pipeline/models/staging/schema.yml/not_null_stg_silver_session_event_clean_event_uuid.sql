
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_uuid
from gold.stg_silver_session_event_clean
where event_uuid is null



  
  
      
    ) dbt_internal_test