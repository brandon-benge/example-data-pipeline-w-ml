
    
    

select
    event_uuid as unique_field,
    count(*) as n_records

from gold.fct_session_events
where event_uuid is not null
group by event_uuid
having count(*) > 1


