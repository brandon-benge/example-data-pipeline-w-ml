
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_id
from gold.stg_silver_order_header
where customer_id is null



  
  
      
    ) dbt_internal_test