
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sales_rep_id
from gold.dim_sales_rep
where sales_rep_id is null



  
  
      
    ) dbt_internal_test