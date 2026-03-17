
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
select *
from gold.fct_campaign_daily
where attributed_revenue < 0

  
  
      
    ) dbt_internal_test