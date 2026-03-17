
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_uuid
from gold.fct_session_events
where event_uuid is null



  
  
      
    ) dbt_internal_test