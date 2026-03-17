
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select advertiser_id
from gold.dim_advertiser
where advertiser_id is null



  
  
      
    ) dbt_internal_test