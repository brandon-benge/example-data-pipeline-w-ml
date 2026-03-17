
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sales_activity_id
from gold.fct_sales_activity
where sales_activity_id is null



  
  
      
    ) dbt_internal_test