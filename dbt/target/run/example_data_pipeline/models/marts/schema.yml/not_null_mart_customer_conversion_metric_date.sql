
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select metric_date
from gold.mart_customer_conversion
where metric_date is null



  
  
      
    ) dbt_internal_test