
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    event_uuid as unique_field,
    count(*) as n_records

from gold.fct_session_events
where event_uuid is not null
group by event_uuid
having count(*) > 1



  
  
      
    ) dbt_internal_test