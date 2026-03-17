
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_token
from gold.mart_customer_conversion
where customer_token is null



  
  
      
    ) dbt_internal_test