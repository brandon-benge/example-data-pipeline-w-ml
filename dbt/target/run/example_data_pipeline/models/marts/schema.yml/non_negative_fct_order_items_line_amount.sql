
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
select *
from gold.fct_order_items
where line_amount < 0

  
  
      
    ) dbt_internal_test