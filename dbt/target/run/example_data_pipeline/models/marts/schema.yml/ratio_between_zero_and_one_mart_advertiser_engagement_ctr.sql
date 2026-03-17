
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
select *
from gold.mart_advertiser_engagement
where ctr < 0
   or ctr > 1

  
  
      
    ) dbt_internal_test